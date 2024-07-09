# Use the built-in version of scandir/walk if possible, otherwise
# use the scandir module version
try:
    from os import scandir
except ImportError:
    from scandir import scandir

import os
from os.path import join, getmtime, relpath, getctime
from . import sync_logging
from .custom_event_handler import custom_event_handler
import irods.keywords as kw
from irods.parallel import _Multipart_close_manager
from .redis_key import sync_time_key_handle
from .sync_utils import size, get_redis
from .sync_task import ContinueException
from .utils import enqueue_task, Operation
import base64
from billiard import current_process
import concurrent.futures
from datetime import datetime
import hashlib
import io
from minio import Minio
import multiprocessing
import re
import redis_lock
import stat
import threading


class scanner(object):
    def __init__(self, meta):
        self.meta = meta.copy()

    def exclude_file_type(self, dir_regex, file_regex, full_path, logger, mode=None):
        ex_list = self.meta["exclude_file_type"]
        if len(ex_list) <= 0 and None == dir_regex and None == file_regex:
            return False

        ret_val = False
        mode = None

        dir_match = None
        for d in dir_regex:
            dir_match = None != d.match(full_path)
            if dir_match == True:
                break

        file_match = None
        for f in file_regex:
            file_match = None != f.match(full_path)
            if file_match == True:
                return True

        try:
            if mode is None:
                mode = os.lstat(full_path).st_mode
        except FileNotFoundError:
            return False

        if stat.S_ISREG(mode):
            if "regular" in ex_list or file_match:
                ret_val = True
        elif stat.S_ISDIR(mode):
            if "directory" in ex_list or dir_match:
                ret_val = True
        elif stat.S_ISCHR(mode):
            if "character" in ex_list or file_match:
                ret_val = True
        elif stat.S_ISBLK(mode):
            if "block" in ex_list or file_match:
                ret_val = True
        elif stat.S_ISSOCK(mode):
            if "socket" in ex_list or file_match:
                ret_val = True
        elif stat.S_ISFIFO(mode):
            if "pipe" in ex_list or file_match:
                ret_val = True
        elif stat.S_ISLNK(mode):
            if "link" in ex_list or file_match:
                ret_val = True

        return ret_val


class filesystem_scanner(scanner):
    def __init__(self, meta):
        super(filesystem_scanner, self).__init__(meta)

    # sync_path
    def instantiate(self, meta):
        meta = self.meta

        meta["queue_name"] = meta["file_queue"]
        enqueue_task(sync_dir, meta)
        itr = scandir(meta["path"])

        return itr

    def itr(self, meta, obj, obj_stats):
        config = meta["config"]
        logger = sync_logging.get_sync_logger(config["log"])
        file_regex = [re.compile(r) for r in meta["exclude_file_name"]]
        dir_regex = [re.compile(r) for r in meta["exclude_directory_name"]]

        full_path = os.path.abspath(obj.path)
        mode = obj.stat(follow_symlinks=False).st_mode

        try:
            if self.exclude_file_type(dir_regex, file_regex, full_path, logger, mode):
                raise ContinueException

            if not obj.is_symlink() and not bool(mode & stat.S_IRGRP):
                logger.error("physical path is not readable [{0}]".format(full_path))
                return full_path, obj_stats

            if obj.is_dir() and not obj.is_symlink() and not obj.is_file():
                sync_dir_meta = meta.copy()
                sync_dir_meta["path"] = full_path
                sync_dir_meta["mtime"] = obj.stat(follow_symlinks=False).st_mtime
                sync_dir_meta["ctime"] = obj.stat(follow_symlinks=False).st_ctime
                sync_dir_meta["queue_name"] = meta["path_queue"]
                enqueue_task(sync_path, sync_dir_meta)
                raise ContinueException

        except Exception as e:
            raise ContinueException

        obj_stats["is_link"] = obj.is_symlink()
        obj_stats["is_socket"] = stat.S_ISSOCK(mode)
        obj_stats["mtime"] = obj.stat(follow_symlinks=False).st_mtime
        obj_stats["ctime"] = obj.stat(follow_symlinks=False).st_ctime
        obj_stats["size"] = obj.stat(follow_symlinks=False).st_size

        return full_path, obj_stats

    # sync_entry
    def construct_path(self, meta, path):
        # target2
        return join(meta["target"], relpath(path, start=meta["root"]))

    def upload_file(self, logger, session, meta, op, exists, **options):
        """
        Function called from sync_irods.sync_file and sync_irods.upload_file for local files

        If called from sync_irods.sync_file, determines if it should be an append operation, or a put.
        If called from sync_irods.upload_file, simply puts the file into iRODS

        op: the type of operation (one of Operation.PUT, Operation.PUT_APPEND, Operation.PUT_SYNC)
        exists: (boolean) specifies whether or not the destination data object already exists within iRODS
        """
        dest_dataobj_logical_fullpath = meta["target"]
        source_physical_fullpath = meta["path"]
        b64_path_str = meta.get("b64_path_str")
        if b64_path_str is not None:
            source_physical_fullpath = base64.b64decode(b64_path_str)

        # PUT_APPEND with existing file
        if op == Operation.PUT_APPEND and exists:
            BUFFER_SIZE = 1024
            logger.info(
                f"appending object {source_physical_fullpath} from local filesystem, options = {options}"
            )
            tsize = size(session, dest_dataobj_logical_fullpath)
            tfd = session.data_objects.open(
                dest_dataobj_logical_fullpath, "a", **options
            )
            tfd.seek(tsize)
            with open(source_physical_fullpath, "rb") as sfd:
                sfd.seek(tsize)
                while True:
                    buf = sfd.read(BUFFER_SIZE)
                    if buf == b"":
                        break
                    tfd.write(buf)
            tfd.close()
            logger.info(
                "succeeded", task="irods_FSappend_file", path=source_physical_fullpath
            )

        # If data object doesn't exist, just do a standard put
        # If data object does exists, we know op=PUT_SYNC, and we re-copy whole file again, so it is fine also
        else:
            logger.info(
                f"uploading object {source_physical_fullpath} from local filesystem, options = {options}"
            )
            session.data_objects.put(
                source_physical_fullpath, dest_dataobj_logical_fullpath, **options
            )
            logger.info(
                "succeeded", task="irods_FSupload_file", path=source_physical_fullpath
            )


class s3_scanner(scanner):
    def __init__(self, meta):
        super(s3_scanner, self).__init__(meta)
        self.proxy_url = meta.get("s3_proxy_url")
        if self.proxy_url is None:
            httpClient = None
        else:
            import urllib3

            httpClient = urllib3.ProxyManager(
                self.proxy_url,
                timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                cert_reqs="CERT_REQUIRED",
                retries=urllib3.Retry(
                    total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]
                ),
            )
        self.endpoint_domain = meta.get("s3_endpoint_domain")
        self.s3_access_key = meta.get("s3_access_key")
        self.s3_secret_key = meta.get("s3_secret_key")
        self.s3_secure_connection = meta.get("s3_secure_connection", True)
        self.client = Minio(
            self.endpoint_domain,
            access_key=self.s3_access_key,
            secret_key=self.s3_secret_key,
            secure=self.s3_secure_connection,
            http_client=httpClient,
        )

    # sync_path
    def instantiate(self, meta):
        path_list = meta["path"].lstrip("/").split("/", 1)
        bucket_name = path_list[0]
        if len(path_list) == 1:
            prefix = ""
        else:
            prefix = path_list[1]
        meta["root"] = bucket_name
        meta["s3_prefix"] = prefix
        itr = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)

        return itr

    def itr(self, meta, obj, obj_stats):
        full_path = obj.object_name

        try:
            if obj.object_name.endswith("/"):
                return full_path, obj_stats
        except Exception as e:
            raise ContinueException

        obj_stats["is_link"] = False
        obj_stats["is_socket"] = False
        obj_stats["mtime"] = obj.last_modified.timestamp()
        obj_stats["ctime"] = obj.last_modified.timestamp()
        obj_stats["size"] = obj.size

        return full_path, obj_stats

    # sync_entry
    def construct_path(self, meta, path):
        # Strip prefix from S3 path
        prefix = meta["s3_prefix"]
        reg_path = path[path.index(prefix) + len(prefix) :].strip("/")
        # Construct S3 "logical path"
        target2 = join(meta["target"], reg_path)
        # Construct S3 "physical path" as: /bucket/objectname
        meta["path"] = "/" + join(meta["root"], path)

        return target2

    #   Upload and Sync Functions
    # --------------------------------------------------------------------------------------------------------------------------------------------------------
    def upload_file(self, logger, session, meta, op, exists, **options):
        """
        Function called from sync_irods.sync_file and sync_irods.upload_file, for S3 objects

        If called from sync_irods.sync_file, determines if it should be an append operation, or a put.
        If called from sync_irods.upload_file, simply puts the file into iRODS

        Parameters:
        op: the type of operation (one of Operation.PUT, Operation.PUT_APPEND, Operation.PUT_SYNC)
        exists: (boolean) specifies whether or not the destination data object already exists within iRODS
        """

        dest_dataobj_logical_fullpath = meta["target"]
        source_physical_fullpath = meta["path"]
        b64_path_str = meta.get("b64_path_str")
        if b64_path_str is not None:
            source_physical_fullpath = base64.b64decode(b64_path_str)
        path_list = source_physical_fullpath.lstrip("/").split("/", 1)
        if len(path_list) == 1:
            raise ValueError(
                "Object name or bucket name missing from path to S3 object: [{}]".format(
                    source_physical_fullpath
                )
            )
        bucket_name = path_list[0]
        object_name = path_list[1]

        MAXIMUM_SINGLE_THREADED_TRANSFER_SIZE = 32 * (1024**2)

        total_bytes = self.client.stat_object(bucket_name, object_name).size
        # Parallelization is only viable for new files, OR if op=PUT_SYNC for existing data objects since we copy the entire file again
        if total_bytes > MAXIMUM_SINGLE_THREADED_TRANSFER_SIZE and (
            not exists or op == Operation.PUT_SYNC
        ):
            target_resc_name = options.get(kw.RESC_NAME_KW, "") or options.get(
                kw.DEST_RESC_NAME_KW, ""
            )
            logger.info(
                "Parallel put",
                task="irods_S3upload_file",
                path=source_physical_fullpath,
            )
            bytecounts = self.parallel_upload_from_S3(
                logger,
                session,
                meta,
                bucket_name,
                object_name,
                dest_dataobj_logical_fullpath,
                total_bytes,
                target_resc_name,
                **options,
            )
            if bytecounts != total_bytes:
                raise RuntimeError(
                    "Parallel put from S3 failed: [{}]".format(source_physical_fullpath)
                )

        # Single stream should happen for files <= 32MiB or for op=PUT_APPEND
        else:
            # Ensures tsize is not None
            tsize = size(session, dest_dataobj_logical_fullpath) or 0
            logger.info(
                "Single stream put",
                task="irods_S3upload_file",
                path=source_physical_fullpath,
            )
            self.single_stream_upload_from_S3(
                logger,
                session,
                meta,
                bucket_name,
                object_name,
                dest_dataobj_logical_fullpath,
                offset=tsize,
                **options,
            )

    def parallel_upload_from_S3(
        self,
        logger,
        session,
        meta,
        bucket_name,
        object_name,
        dest,
        total_bytes,
        target_resc_name="",
        **options,
    ):
        """
        Sets up threads to parallelize copying of S3 file into iRODS. Uses PRC _Multipart_close_manager to ensure
        initial transfer thread is also the last one to call the close method on its 'Io' object.

        Each thread calls thread_copy_chunk_from_S3, which is where the work to copy the S3 object is done
        """
        RECOMMENDED_NUM_THREADS_PER_TRANSFER = 3
        num_threads = max(
            1, min(multiprocessing.cpu_count(), RECOMMENDED_NUM_THREADS_PER_TRANSFER)
        )
        open_options = {
            kw.NUM_THREADS_KW: str(num_threads),
            kw.DATA_SIZE_KW: str(total_bytes),
        }
        if target_resc_name:
            open_options[kw.RESC_NAME_KW] = target_resc_name
            open_options[kw.DEST_RESC_NAME_KW] = target_resc_name

        # initial open
        (Io, rawfile) = session.data_objects.open_with_FileRaw(
            dest, "w", finalize_on_close=True, **open_options
        )
        # need replica token so that when other threads have permission to open and write to the destination irods data object
        (replica_token, resc_hier) = rawfile.replica_access_info()

        # Separating file into chunks of equal size for each thread
        bytes_per_thread = total_bytes // num_threads
        # Creating a range of bytes for each thread
        chunk_ranges = [
            range(j * bytes_per_thread, (j + 1) * bytes_per_thread)
            for j in range(num_threads - 1)
        ]
        # The last thread's range should end at the size of the file: total_bytes
        chunk_ranges.append(range((num_threads - 1) * bytes_per_thread, total_bytes))

        futures = []
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
        num_threads = min(num_threads, len(chunk_ranges))
        mgr = _Multipart_close_manager(Io, threading.Barrier(num_threads))
        ops = {}
        ops[kw.NUM_THREADS_KW] = str(num_threads)
        ops[kw.DATA_SIZE_KW] = str(total_bytes)
        ops[kw.RESC_HIER_STR_KW] = resc_hier
        ops[kw.REPLICA_TOKEN_KW] = replica_token

        # Loop to open an Io object for each of the threads and call the thread copy function
        # The initial thread uses the Io from above
        for thread_id, byte_range in enumerate(chunk_ranges):
            if Io is None:
                Io = session.data_objects.open(
                    dest, "a", create=False, finalize_on_close=False, **ops
                )
            mgr.add_io(Io)
            futures.append(
                executor.submit(
                    self.thread_copy_chunk_from_S3,
                    Io,
                    byte_range,
                    bucket_name,
                    object_name,
                    mgr,
                    str(thread_id),
                )
            )
            Io = None
        bytecounts = sum([f.result() for f in futures])
        return bytecounts

    def thread_copy_chunk_from_S3(
        self,
        dst_data_object,
        chunk_range,
        bucket_name,
        object_name,
        dst_chunk_mgr,
        thread_debug_id,
    ):
        """
        Called by the threads in parallel_upload_from_S3. This is where the actual copying from the S3 object to the iRODS data object happens.

        Parameters:
        dst_data_object (io.BufferedRandom): This is the object handle for the destination data object in iRODS. Each thread that calls this function will have one that it can seek and write to.
        chunk_range (range): This is the range of bytes that a given thread is supposed to copy.
        bucket_name (str): The name of the S3 bucket that is ingested
        object_name (str): The name of the S3 object that will be copied to iRODS
        dst_chunk_mgr (_Multipart_close_manager): Object to organize threading (read upload_file docstring for more information or parallel.py in PRC)

        Returns:
        int: Total number of bytes written. This is used as a sanity check to ensure all bytes of the source S3 object were written.
        """
        COPY_BUF_SIZE = (1024**2) * 4
        offset = chunk_range[0]
        length = len(chunk_range)
        dst_data_object.seek(offset)
        response = self.client.get_object(
            bucket_name, object_name, offset=offset, length=length
        )
        bytecount = 0
        for data in response.stream(amt=COPY_BUF_SIZE):
            dst_data_object.write(data)
            bytecount += len(data)

        response.release_conn()
        dst_chunk_mgr.remove_io(dst_data_object)
        return bytecount

    def single_stream_upload_from_S3(
        self, logger, session, meta, bucket_name, object_name, dest, offset=0, **options
    ):
        """
        This is the non-parallel upload or append. It works by simply streaming through the source S3 object and writing it to disk.
        """

        # 1024*1024
        buffer_size = 1024 * io.DEFAULT_BUFFER_SIZE

        # Pulled from put function from data_object_manager in PRC
        # Set operation type to trigger acPostProcForPut
        if kw.OPR_TYPE_KW not in options:
            options[kw.OPR_TYPE_KW] = 1  # PUT_OPR

        # Add checksum calculation for opening file
        if kw.REG_CHKSUM_KW not in options:
            options[kw.REG_CHKSUM_KW] = ""

        if offset != 0:
            tfd = session.data_objects.open(dest, "a", **options)
            tfd.seek(offset)
            buffer_size = 1024
        else:
            tfd = session.data_objects.open(dest, "w", **options)

        response = self.client.get_object(bucket_name, object_name, offset=offset)
        etag = response.getheader("Etag").replace('"', "")
        recreated_etag = None
        # Preliminary way of determining if file is multipart or not; etag from amazonS3 will be >32 for multipart, 32 for regular
        # If offset, means we are appending, so treat as normal non-multipart
        if len(etag) > 32 and offset == 0:
            logger.info(
                "Multipart object",
                task="irods_S3upload_file",
                path="/" + bucket_name + "/" + object_name,
            )
            c_size = meta.get("s3_multipart_chunksize_in_mib", 8)
            chksums = []
            # Calculating md5 sum for each chunk
            for data in response.stream(amt=c_size * 1024 * 1024):
                tfd.write(data)
                hash_md5 = hashlib.md5()
                hash_md5.update(data)
                chksums.append(hash_md5)

            tfd.close()
            # byte string so need b
            digests = b"".join(c.digest() for c in chksums)
            # Checksum of concatenated checksums
            hash_md5 = hashlib.md5()
            hash_md5.update(digests)
            # Number of parts in multi-part upload is added with '-' to the end
            n_parts = len(chksums)
            if n_parts > 1:
                recreated_etag = hash_md5.hexdigest() + "-" + str(n_parts)
            else:
                logger.error(
                    "Incorrect multipart chunksize or error computing multipart checksum",
                    task="irods_S3upload_file",
                    path="/" + bucket_name + "/" + object_name,
                )
        else:
            logger.info(
                "Non-multipart object",
                task="irods_S3upload_file",
                path="/" + bucket_name + "/" + object_name,
            )
            hash_md5 = hashlib.md5()
            for data in response.stream(amt=buffer_size):
                hash_md5.update(data)
                tfd.write(data)
            tfd.close()
            recreated_etag = hash_md5.hexdigest()

        response.release_conn()
        # need to check offset = 0 because md5hash for Operation.PUT_APPEND will not match etag
        if etag != recreated_etag and offset == 0:
            logger.error(
                "checksums do not match! etag {} recreated etag {}".format(
                    etag, recreated_etag
                ),
                task="irods_S3upload_file",
                path="/" + bucket_name + "/" + object_name,
            )


# Attempt to encode full physical path on local filesystem
# Special handling required for non-encodable strings which raise UnicodeEncodeError
def is_unicode_encode_error_path(path):
    try:
        _ = path.encode("utf8")
    except UnicodeEncodeError:
        return True
    return False


def scanner_factory(meta):
    if meta.get("s3_keypair"):
        return s3_scanner(meta)
    return filesystem_scanner(meta)


# at bottom for circular dependency issues
from .sync_task import sync_files, sync_dir, sync_path
