from . import irods_utils
from .. import custom_event_handler
from ..utils import Operation

from irods.models import Resource, DataObject, Collection
import irods.keywords as kw

from minio import Minio

import base64
import hashlib
import io
import os


def parallel_upload_from_S3(
    s3_client,
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
                thread_copy_chunk_from_S3,
                s3_client,
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
    s3_client,
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
    response = s3_client.get_object(
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
    s3_client,
    logger,
    session,
    meta,
    bucket_name,
    object_name,
    dest,
    offset=0,
    **options,
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

    response = s3_client.get_object(bucket_name, object_name, offset=offset)
    # urllib3 replaced HTTPResponse.getheader with HTTPResponse.headers.get in 2.6.0.
    if hasattr(response, "getheader"):
        etag = response.getheader("Etag").replace('"', "")
    else:
        etag = response.headers.get("Etag").replace('"', "")
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


def register_file(hdlr_mod, logger, session, meta, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    b64_path_str = meta.get("b64_path_str")

    event_handler = custom_event_handler.custom_event_handler(meta)
    if event_handler is None:
        phypath_to_register_in_catalog = None
    else:
        phypath_to_register_in_catalog = event_handler.target_path(session, **options)
    if phypath_to_register_in_catalog is None:
        if b64_path_str is not None and "unicode_error_filename" in meta:
            phypath_to_register_in_catalog = os.path.join(
                source_physical_fullpath, meta["unicode_error_filename"]
            )
        else:
            phypath_to_register_in_catalog = source_physical_fullpath

    resc_name = event_handler.to_resource(session, **options)
    if resc_name is not None:
        options["destRescName"] = resc_name

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    options[kw.DATA_SIZE_KW] = str(meta["size"])
    options[kw.DATA_MODIFY_KW] = str(int(meta["mtime"]))

    logger.info(
        "registering object {}, source_physical_fullpath: {}, options = {}".format(
            dest_dataobj_logical_fullpath, source_physical_fullpath, options
        )
    )
    session.data_objects.register(
        phypath_to_register_in_catalog, dest_dataobj_logical_fullpath, **options
    )

    logger.info("succeeded", task="irods_register_file", path=source_physical_fullpath)

    irods_utils.annotate_metadata_for_special_data_objs(
        meta, session, source_physical_fullpath, dest_dataobj_logical_fullpath
    )


def _upload_file(logger, session, meta, op, exists=False, **options):
    """
    Function called from sync_irods.sync_file and sync_irods.upload_file, for S3 objects

    If called from sync_irods.sync_file, determines if it should be an append operation, or a put.
    If called from sync_irods.upload_file, simply puts the file into iRODS

    Parameters:
    op: the type of operation (one of Operation.PUT, Operation.PUT_APPEND, Operation.PUT_SYNC)
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

    # TODO: Pull this out into a configuration
    MAXIMUM_SINGLE_THREADED_TRANSFER_SIZE = 32 * (1024**2)

    # TODO: How do we avoid doing this everywhere?
    proxy_url = meta.get("s3_proxy_url")
    if proxy_url is None:
        httpClient = None
    else:
        import urllib3

        httpClient = urllib3.ProxyManager(
            proxy_url,
            timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
            cert_reqs="CERT_REQUIRED",
            retries=urllib3.Retry(
                total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]
            ),
        )
    endpoint_domain = meta.get("s3_endpoint_domain")
    s3_access_key = meta.get("s3_access_key")
    s3_secret_key = meta.get("s3_secret_key")
    s3_secure_connection = meta.get("s3_secure_connection", True)
    s3_client = Minio(
        endpoint_domain,
        access_key=s3_access_key,
        secret_key=s3_secret_key,
        secure=s3_secure_connection,
        http_client=httpClient,
    )

    total_bytes = s3_client.stat_object(bucket_name, object_name).size
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
        bytecounts = parallel_upload_from_S3(
            s3_client,
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
        tsize = irods_utils.size(session, dest_dataobj_logical_fullpath) or 0
        logger.info(
            "Single stream put",
            task="irods_S3upload_file",
            path=source_physical_fullpath,
        )
        single_stream_upload_from_S3(
            s3_client,
            logger,
            session,
            meta,
            bucket_name,
            object_name,
            dest_dataobj_logical_fullpath,
            offset=tsize,
            **options,
        )


def upload_file(hdlr_mod, logger, session, meta, op, **options):
    _upload_file(logger, session, meta, op, exists=False, **options)


def no_op(hdlr_mod, logger, session, meta, **options):
    pass


def sync_file(hdlr_mod, logger, session, meta, scanner, op, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    b64_path_str = meta.get("b64_path_str")

    event_handler = custom_event_handler.custom_event_handler(meta)
    resc_name = event_handler.to_resource(session, **options)
    if resc_name is not None:
        options["destRescName"] = resc_name

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    logger.info(
        "syncing object %s, options = %s" % (dest_dataobj_logical_fullpath, options)
    )

    # TODO(#208): Investigate behavior of sync_file when op is None
    if op is None:
        op = Operation.REGISTER_SYNC

    # Use scanner object to sync files
    # Ensure exists=True so that upload_file understands that it is a sync_file operation
    _upload_file(logger, session, meta, op, exists=True, **options)


def update_metadata(hdlr_mod, logger, session, meta, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    event_handler = custom_event_handler.custom_event_handler(meta)
    phypath_to_register_in_catalog = event_handler.target_path(session, **options)
    b64_path_str = meta.get("b64_path_str")
    if phypath_to_register_in_catalog is None:
        if b64_path_str is not None and "unicode_error_filename" in meta:
            # Append generated filename to truncated fullpath because it failed to encode
            # TODO(#250): This will not work on Windows.
            phypath_to_register_in_catalog = os.path.join(
                source_physical_fullpath, meta["unicode_error_filename"]
            )
        else:
            phypath_to_register_in_catalog = source_physical_fullpath

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    size = int(meta["size"])
    mtime = int(meta["mtime"])
    logger.info(
        f"updating object: {dest_dataobj_logical_fullpath}, options = {options}"
    )

    data_obj_info = {"objPath": dest_dataobj_logical_fullpath}

    outdated_repl_nums = []
    found = False

    resc_name = event_handler.to_resource(session, **options)
    if resc_name is None:
        found = True
    else:
        for row in session.query(
            Resource.name, DataObject.path, DataObject.replica_number
        ).filter(
            # TODO(#250): This will not work on Windows.
            DataObject.name == os.path.basename(dest_dataobj_logical_fullpath),
            Collection.name == os.path.dirname(dest_dataobj_logical_fullpath),
        ):
            if row[DataObject.path] == phypath_to_register_in_catalog:
                if irods_utils.child_of(session, row[Resource.name], resc_name):
                    found = True
                    repl_num = row[DataObject.replica_number]
                    data_obj_info["replNum"] = repl_num
                    continue

    if not found:
        if b64_path_str is not None:
            logger.error(
                "updating object: wrong resource or path, "
                "dest_dataobj_logical_fullpath = {}, phypath_to_register_in_catalog = {}, options = {}".format(
                    dest_dataobj_logical_fullpath,
                    phypath_to_register_in_catalog,
                    str(options),
                )
            )
        else:
            logger.error(
                "updating object: wrong resource or path, "
                "dest_dataobj_logical_fullpath = {}, source_physical_fullpath = {}, "
                "phypath_to_register_in_catalog = {}, options = {}".format(
                    dest_dataobj_logical_fullpath,
                    source_physical_fullpath,
                    phypath_to_register_in_catalog,
                    str(options),
                )
            )
        raise Exception("wrong resource or path")

    session.data_objects.modDataObjMeta(
        data_obj_info,
        {"dataSize": size, "dataModify": mtime, "allReplStatus": 1},
        **options,
    )

    if b64_path_str is not None:
        logger.info(
            "succeeded",
            task="irods_update_metadata",
            path=phypath_to_register_in_catalog,
        )
    else:
        logger.info(
            "succeeded", task="irods_update_metadata", path=source_physical_fullpath
        )


def sync_file_meta(hdlr_mod, logger, session, meta, **options):
    pass


def sync_data_from_file(hdlr_mod, meta, logger, content, **options):
    target = meta["target"]
    path = meta["path"]

    event_handler = custom_event_handler.custom_event_handler(meta)
    session = irods_utils.irods_session(
        event_handler.get_module(), meta, logger, **options
    )

    if meta.get("initial_ingest"):
        # If the initial_ingest option has been specified, no checking is done for the existence
        # of the logical path for performance reasons. If the option is specified and the logical
        # path exists, errors may occur; this behavior is expected.
        exists = False
    else:
        exists = session.data_objects.exists(target)
        if not exists and session.collections.exists(target):
            raise Exception(f"sync: cannot sync file {path} to collection {target}")

    op = event_handler.operation(session, **options)

    if op == Operation.NO_OP:
        if not exists:
            event_handler.call(
                "on_data_obj_create", logger, no_op, logger, session, meta, **options
            )
        else:
            event_handler.call(
                "on_data_obj_modify", logger, no_op, logger, session, meta, **options
            )
    else:
        # allow_redirect will cause PRC to establish a direct connection between the client and the server hosting the
        # resource to which the data is being uploaded. This can cause problems if the hostnames being used in the
        # client environment and the hostname used for the "location" of the resource differ despite referring to the
        # same host. As such, we set the allow_redirect option to False in order to prevent this redirect.
        options["allow_redirect"] = False

        createRepl = False
        if op is None:
            op = Operation.REGISTER_SYNC
        elif exists and op == Operation.REGISTER_AS_REPLICA_SYNC:
            resc_name = event_handler.to_resource(session, **options)
            if resc_name is None:
                raise Exception("no resource name defined")

            found = False
            foundPath = False
            for replica in session.data_objects.get(target).replicas:
                if irods_utils.child_of(session, replica.resource_name, resc_name):
                    found = True
                    if replica.path == path:
                        foundPath = True
            if not found:
                createRepl = True
            elif not foundPath:
                raise Exception(
                    f"Data object [{target}] has at least one replica under resource [{resc_name}], but none of the replicas have physical paths which match [{path}]."
                )

        put = op in [Operation.PUT, Operation.PUT_SYNC, Operation.PUT_APPEND]

        if not exists:
            meta2 = meta.copy()
            # TODO(#250): This will not work on Windows.
            meta2["target"] = os.path.dirname(target)
            if "b64_path_str" not in meta2:
                # TODO: This will not work on Windows.
                meta2["path"] = os.path.dirname(path)
            irods_utils.create_dirs(logger, session, meta2, **options)
            if put:
                event_handler.call(
                    "on_data_obj_create",
                    logger,
                    upload_file,
                    logger,
                    session,
                    meta,
                    op,
                    **options,
                )
            else:
                event_handler.call(
                    "on_data_obj_create",
                    logger,
                    register_file,
                    logger,
                    session,
                    meta,
                    **options,
                )
        elif createRepl:
            options["regRepl"] = ""

            event_handler.call(
                "on_data_obj_create",
                logger,
                register_file,
                logger,
                session,
                meta,
                **options,
            )
        elif content:
            if put:
                if Operation.PUT == op:
                    logger.debug(
                        f"PUT operation will ignore existing data object [{meta['target']}]"
                    )
                else:
                    # PUT_SYNC and PUT_APPEND sync data on existing data objects.
                    event_handler.call(
                        "on_data_obj_modify",
                        logger,
                        sync_file,
                        logger,
                        session,
                        meta,
                        op,
                        **options,
                    )
            else:
                event_handler.call(
                    "on_data_obj_modify",
                    logger,
                    update_metadata,
                    logger,
                    session,
                    meta,
                    **options,
                )
        else:
            event_handler.call(
                "on_data_obj_modify",
                logger,
                sync_file_meta,
                logger,
                session,
                meta,
                **options,
            )

    irods_utils.start_timer()


def sync_metadata_from_file(hdlr_mod, meta, logger, **options):
    sync_data_from_file(hdlr_mod, meta, logger, False, **options)


def sync_dir_meta(hdlr_mod, logger, session, meta, **options):
    pass


def sync_data_from_dir(hdlr_mod, meta, logger, content, **options):
    target = meta["target"]
    path = meta["path"]

    event_handler = custom_event_handler.custom_event_handler(meta)
    session = irods_utils.irods_session(
        event_handler.get_module(), meta, logger, **options
    )
    exists = session.collections.exists(target)

    # TODO(#208): Should we default to REGISTER_SYNC?
    op = event_handler.operation(session, **options) or Operation.REGISTER_SYNC
    if op == Operation.NO_OP:
        operation_name = "on_coll_modify" if exists else "on_coll_create"
        event_handler.call(
            operation_name, logger, no_op, logger, session, meta, **options
        )
    else:
        if not exists:
            irods_utils.create_dirs(logger, session, meta, **options)
        else:
            event_handler.call(
                "on_coll_modify",
                logger,
                sync_dir_meta,
                logger,
                session,
                meta,
                **options,
            )
    irods_utils.start_timer()


def sync_metadata_from_dir(hdlr_mod, meta, logger, **options):
    sync_data_from_dir(hdlr_mod, meta, logger, False, **options)
