# Use the built-in version of scandir/walk if possible, otherwise
# use the scandir module version
try:
    from os import scandir
except ImportError:
    from scandir import scandir

import stat
import os
from os.path import join, getmtime, relpath, getctime
from datetime import datetime
import redis_lock
from . import sync_logging
from .custom_event_handler import custom_event_handler
from .redis_key import sync_time_key_handle
from .sync_utils import get_redis
from .utils import enqueue_task
from minio import Minio
from billiard import current_process
import base64
import re


class scanner(object):
    def __init__(self, meta):
        self.meta = meta.copy()

    def exclude_file_type(self, dir_regex, file_regex, full_path, logger, mode=None):
        ex_list = self.meta['exclude_file_type']
        if len(ex_list) <= 0 and None == dir_regex and None == file_regex:
            return False

        ret_val = False
        mode = None

        dir_match = None
        for d in dir_regex:
            dir_match = None != d.match(full_path)
            if(dir_match == True):
                break

        file_match = None
        for f in file_regex:
            file_match = None != f.match(full_path)
            if(file_match == True):
                break

        try:
            if mode is None:
                mode = os.lstat(full_path).st_mode
        except FileNotFoundError:
            return False

        #TODO
        if stat.S_ISREG(mode):
            if 'regular' in ex_list or file_match:
                ret_val = True
        elif stat.S_ISDIR(mode):
            if 'directory' in ex_list or dir_match:
                ret_val = True
        elif stat.S_ISCHR(mode):
            if 'character' in ex_list or file_match:
                ret_val = True
        elif stat.S_ISBLK(mode):
            if 'block' in ex_list or file_match:
                ret_val = True
        elif stat.S_ISSOCK(mode):
            if 'socket' in ex_list or file_match:
                ret_val = True
        elif stat.S_ISFIFO(mode):
            if 'pipe' in ex_list or file_match:
                ret_val = True
        elif stat.S_ISLNK(mode):
            if 'link' in ex_list or file_match:
                ret_val = True

        return ret_val


class filesystem_scanner(scanner):
    def __init__(self, meta):
        super(filesystem_scanner, self).__init__(meta)

    def sync_path(self, task_cls, meta):
        path = meta["path"]
        config = meta["config"]
        logging_config = config["log"]
        exclude_file_name = meta['exclude_file_name']
        exclude_directory_name = meta['exclude_directory_name']

        logger = sync_logging.get_sync_logger(logging_config)

        file_regex = [re.compile(r) for r in exclude_file_name]
        dir_regex = [re.compile(r) for r in exclude_directory_name]

        try:
            logger.info("walk dir", path=path)
            meta = meta.copy()
            meta["task"] = "sync_dir"
            chunk = {}

# ----------------------
            meta['queue_name'] = meta["file_queue"]
            enqueue_task(sync_dir, meta)
            itr = scandir(path)
# ----------------------

            if meta["profile"]:
                #config = meta["config"]
                profile_log = config.get("profile")
                profile_logger = sync_logging.get_sync_logger(profile_log)
                task_id = task_cls.request.id

                profile_logger.info("list_dir_prerun", event_id=task_id + ":list_dir", event_name="list_dir",
                                    hostname=task_cls.request.hostname, index=current_process().index)
                itr = list(itr)
                if meta["profile"]:
                    profile_logger.info("list_dir_postrun", event_id=task_id + ":list_dir", event_name="list_dir",
                                        hostname=task_cls.request.hostname, index=current_process().index)

            for obj in itr:
                obj_stats = {}
# ----------------------
                full_path = os.path.abspath(obj.path)
                mode = obj.stat(follow_symlinks=False).st_mode

                if self.exclude_file_type(dir_regex, file_regex, full_path, logger, mode):
                    continue

                if not obj.is_symlink() and not bool(mode & stat.S_IRGRP):
                    logger.error(
                        'physical path is not readable [{0}]'.format(full_path))
                    continue

                if obj.is_dir() and not obj.is_symlink() and not obj.is_file():
                    sync_dir_meta = meta.copy()
                    sync_dir_meta['path'] = full_path
                    sync_dir_meta['mtime'] = obj.stat(
                        follow_symlinks=False).st_mtime
                    sync_dir_meta['ctime'] = obj.stat(
                        follow_symlinks=False).st_ctime
                    sync_dir_meta['queue_name'] = meta["path_queue"]
                    enqueue_task(sync_path, sync_dir_meta)
                    continue

                obj_stats['is_link'] = obj.is_symlink()
                obj_stats['is_socket'] = stat.S_ISSOCK(mode)
                obj_stats['mtime'] = obj.stat(follow_symlinks=False).st_mtime
                obj_stats['ctime'] = obj.stat(follow_symlinks=False).st_ctime
                obj_stats['size'] = obj.stat(follow_symlinks=False).st_size
# ----------------------

                # add object stat dict to the chunk dict
                chunk[full_path] = obj_stats

                # Launch async job when enough objects are ready to be sync'd
                files_per_task = meta.get('files_per_task')
                if len(chunk) >= files_per_task:
                    sync_files_meta = meta.copy()
                    sync_files_meta['chunk'] = chunk
                    sync_files_meta['queue_name'] = meta["file_queue"]
                    enqueue_task(sync_files, sync_files_meta)
                    chunk.clear()

            if len(chunk) > 0:
                sync_files_meta = meta.copy()
                sync_files_meta['chunk'] = chunk
                sync_files_meta['queue_name'] = meta["file_queue"]
                enqueue_task(sync_files, sync_files_meta)
                chunk.clear()

        except Exception as err:
            event_handler = custom_event_handler(meta)
            retry_countdown = event_handler.delay(task_cls.request.retries + 1)
            max_retries = event_handler.max_retries()
            raise task_cls.retry(max_retries=max_retries,
                                 exc=err, countdown=retry_countdown)

    def sync_entry(self, task_cls, meta, cls, datafunc, metafunc):
        path = meta["path"]
        target = meta["target"]
        config = meta["config"]
        logging_config = config["log"]
        logger = sync_logging.get_sync_logger(logging_config)

        event_handler = custom_event_handler(meta)
        max_retries = event_handler.max_retries()

        lock = None

        logger.info("synchronizing " + cls + ". path = " + path)

        if is_unicode_encode_error_path(path):
            #encodes to utf8 and logs warning
            abspath = os.path.abspath(path)
            path = os.path.dirname(abspath)
            utf8_escaped_abspath = abspath.encode('utf8', 'surrogateescape')
            b64_path_str = base64.b64encode(utf8_escaped_abspath)
            unicode_error_filename = 'irods_UnicodeEncodeError_' + \
                        str(b64_path_str.decode('utf8'))

            logger.warning(
                        'sync_entry raised UnicodeEncodeError while syncing path:' + str(utf8_escaped_abspath))

            meta['path'] = path
            meta['b64_path_str'] = b64_path_str
            meta['unicode_error_filename'] = unicode_error_filename
            sync_key = str(b64_path_str.decode('utf8')) + ":" + target
        else:
            sync_key = path + ":" + target

        try:
            r = get_redis(config)
            lock = redis_lock.Lock(r, "sync_" + cls + ":"+sync_key)
            lock.acquire()

            sync_time_handle = sync_time_key_handle(r, sync_key)
            if not meta["ignore_cache"]:
                sync_time = sync_time_handle.get_value()
            else:
                sync_time = None

            mtime = meta.get("mtime")
            if mtime is None:
                mtime = getmtime(path)

            ctime = meta.get("ctime")
            if ctime is None:
                ctime = getctime(path)

            if sync_time is not None and mtime < sync_time and ctime < sync_time:
                logger.info("succeeded_" + cls +
                            "_has_not_changed", task=meta["task"], path=path)
            else:
                t = datetime.now().timestamp()
                logger.info("synchronizing " + cls, path=path,
                            t0=sync_time, t=t, ctime=ctime)
                meta2 = meta.copy()
                if path == meta["root"]:
                    if 'unicode_error_filename' in meta:
                        target2 = join(target, meta['unicode_error_filename'])
                    else:
                        target2 = target
                else:
# ----------------------
                    target2 = join(target, relpath(path, start=meta["root"]))
# ----------------------
                meta2["target"] = target2
                if sync_time is None or mtime >= sync_time:
                    datafunc(meta2, logger, True)
                    logger.info("succeeded", task=meta["task"], path=path)
                else:
                    metafunc(meta2, logger)
                    logger.info("succeeded_metadata_only",
                                task=meta["task"], path=path)
                sync_time_handle.set_value(str(t))
        except Exception as err:
            event_handler = custom_event_handler(meta)
            retry_countdown = event_handler.delay(task_cls.request.retries + 1)
            raise task_cls.retry(max_retries=max_retries,
                                 exc=err, countdown=retry_countdown)
        finally:
            if lock is not None:
                lock.release()


class s3_scanner(scanner):
    def __init__(self, meta):
        super(s3_scanner, self).__init__(meta)
        self.proxy_url = meta.get('s3_proxy_url')
        self.endpoint_domain = meta.get('s3_endpoint_domain')
        self.s3_access_key = meta.get('s3_access_key')
        self.s3_secret_key = meta.get('s3_secret_key')

    def sync_path(self, task_cls, meta):
        config = meta["config"]
        logging_config = config["log"]
        exclude_file_name = meta['exclude_file_name']
        exclude_directory_name = meta['exclude_directory_name']

        logger = sync_logging.get_sync_logger(logging_config)

        file_regex = [re.compile(r) for r in exclude_file_name]
        dir_regex = [re.compile(r) for r in exclude_directory_name]

        try:
            logger.info("walk dir", path=meta["path"])
            meta = meta.copy()
            meta["task"] = "sync_dir"
            chunk = {}

# ----------------------
            # instantiate s3 client
            proxy_url = meta.get('s3_proxy_url')
            if proxy_url is None:
                httpClient = None
            else:
                import urllib3
                httpClient = urllib3.ProxyManager(
                    proxy_url,
                    timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                    cert_reqs='CERT_REQUIRED',
                    retries=urllib3.Retry(
                        total=5,
                        backoff_factor=0.2,
                        status_forcelist=[
                            500, 502, 503, 504]
                    )
                )
            endpoint_domain = meta.get('s3_endpoint_domain')
            s3_access_key = meta.get('s3_access_key')
            s3_secret_key = meta.get('s3_secret_key')
            s3_secure_connection = meta.get('s3_secure_connection')
            if s3_secure_connection is None:
                s3_secure_connection = True
            client = Minio(
                endpoint_domain,
                access_key=s3_access_key,
                secret_key=s3_secret_key,
                secure=s3_secure_connection,
                http_client=httpClient)

            # Split provided path into bucket and source folder "prefix"
            path_list = meta["path"].lstrip('/').split('/', 1)
            bucket_name = path_list[0]
            if len(path_list) == 1:
                prefix = ''
            else:
                prefix = path_list[1]
            meta['root'] = bucket_name
            meta['s3_prefix'] = prefix
            itr = client.list_objects(
                bucket_name, prefix=prefix, recursive=True)
# ----------------------

            if meta["profile"]:
                config = meta["config"]
                profile_log = config.get("profile")
                profile_logger = sync_logging.get_sync_logger(profile_log)
                task_id = task_cls.request.id

                profile_logger.info("list_dir_prerun", event_id=task_id + ":list_dir", event_name="list_dir",
                                    hostname=task_cls.request.hostname, index=current_process().index)
                itr = list(itr)
                if meta["profile"]:
                    profile_logger.info("list_dir_postrun", event_id=task_id + ":list_dir", event_name="list_dir",
                                        hostname=task_cls.request.hostname, index=current_process().index)

            for obj in itr:
                obj_stats = {}
# ----------------------
                if obj.object_name.endswith('/'):
                    continue
                full_path = obj.object_name
                obj_stats['is_link'] = False
                obj_stats['is_socket'] = False
                obj_stats['mtime'] = obj.last_modified.timestamp()
                obj_stats['ctime'] = obj.last_modified.timestamp()
                obj_stats['size'] = obj.size
# ----------------------

                # add object stat dict to the chunk dict
                chunk[full_path] = obj_stats

                # Launch async job when enough objects are ready to be sync'd
                files_per_task = meta.get('files_per_task')

                #TODO: simplify
                if len(chunk) >= files_per_task:
                    sync_files_meta = meta.copy()
                    sync_files_meta['chunk'] = chunk
                    sync_files_meta['queue_name'] = meta["file_queue"]
                    enqueue_task(sync_files, sync_files_meta)
                    chunk.clear()

            if len(chunk) > 0:
                sync_files_meta = meta.copy()
                sync_files_meta['chunk'] = chunk
                sync_files_meta['queue_name'] = meta["file_queue"]
                enqueue_task(sync_files, sync_files_meta)
                chunk.clear()

        except Exception as err:
            event_handler = custom_event_handler(meta)
            retry_countdown = event_handler.delay(task_cls.request.retries + 1)
            max_retries = event_handler.max_retries()
            raise task_cls.retry(max_retries=max_retries,
                                 exc=err, countdown=retry_countdown)

    def sync_entry(self, task_cls, meta, cls, datafunc, metafunc):
        path = meta["path"]
        target = meta["target"]
        config = meta["config"]
        logging_config = config["log"]
        logger = sync_logging.get_sync_logger(logging_config)

        event_handler = custom_event_handler(meta)
        max_retries = event_handler.max_retries()

        lock = None

        logger.info("synchronizing " + cls + ". path = " + path)

        if is_unicode_encode_error_path(path):
            #encodes to utf8 and logs warning
            abspath = os.path.abspath(path)
            path = os.path.dirname(abspath)
            utf8_escaped_abspath = abspath.encode('utf8', 'surrogateescape')
            b64_path_str = base64.b64encode(utf8_escaped_abspath)
            unicode_error_filename = 'irods_UnicodeEncodeError_' + \
                        str(b64_path_str.decode('utf8'))

            logger.warning(
                        'sync_entry raised UnicodeEncodeError while syncing path:' + str(utf8_escaped_abspath))

            meta['path'] = path
            meta['b64_path_str'] = b64_path_str
            meta['unicode_error_filename'] = unicode_error_filename
            sync_key = str(b64_path_str.decode('utf8')) + ":" + target
        else:
            sync_key = path + ":" + target

        try:
            r = get_redis(config)
            lock = redis_lock.Lock(r, "sync_" + cls + ":"+sync_key)
            lock.acquire()

            sync_time_handle = sync_time_key_handle(r, sync_key)
            if not meta["ignore_cache"]:
                sync_time = sync_time_handle.get_value()
            else:
                sync_time = None

            mtime = meta.get("mtime")
            if mtime is None:
                mtime = getmtime(path)

            ctime = meta.get("ctime")

            if ctime is None:
                ctime = getctime(path)

            if sync_time is not None and mtime < sync_time and ctime < sync_time:
                logger.info("succeeded_" + cls +
                            "_has_not_changed", task=meta["task"], path=path)
            else:
                t = datetime.now().timestamp()
                logger.info("synchronizing " + cls, path=path,
                            t0=sync_time, t=t, ctime=ctime)
                meta2 = meta.copy()
                if path == meta["root"]:
                    if 'unicode_error_filename' in meta:
                        target2 = join(target, meta['unicode_error_filename'])
                    else:
                        target2 = target
                else:
# ----------------------
                    # Strip prefix from S3 path
                    prefix = meta['s3_prefix']
                    reg_path = path[path.index(
                        prefix) + len(prefix):].strip('/')
                    # Construct S3 "logical path"
                    target2 = join(target, reg_path)
                    # Construct S3 "physical path" as: /bucket/objectname
                    meta2['path'] = '/' + join(meta["root"], path)
# ----------------------
                meta2["target"] = target2
                if sync_time is None or mtime >= sync_time:
                    datafunc(meta2, logger, True)
                    logger.info("succeeded", task=meta["task"], path=path)
                else:
                    metafunc(meta2, logger)
                    logger.info("succeeded_metadata_only",
                                task=meta["task"], path=path)
                sync_time_handle.set_value(str(t))
        except Exception as err:
            event_handler = custom_event_handler(meta)
            retry_countdown = event_handler.delay(task_cls.request.retries + 1)
            raise task_cls.retry(max_retries=max_retries,
                                 exc=err, countdown=retry_countdown)
        finally:
            if lock is not None:
                lock.release()
    

# Attempt to encode full physical path on local filesystem
# Special handling required for non-encodable strings which raise UnicodeEncodeError
def is_unicode_encode_error_path(path):
    try:
        _ = path.encode('utf8')
    except UnicodeEncodeError:
        return True
    return False

def scanner_factory(meta):
    if meta.get('s3_keypair'):
        return s3_scanner(meta)
    return filesystem_scanner(meta)

#at bottom for circular dependency issues 
from .sync_task import sync_files, sync_dir, sync_path
