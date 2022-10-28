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
from .sync_task import ContinueException

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
                return True

        try:
            if mode is None:
                mode = os.lstat(full_path).st_mode
        except FileNotFoundError:
            return False

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
        
    # sync_path
    def instantiate(self, meta):
        meta = self.meta

        meta['queue_name'] = meta["file_queue"]
        enqueue_task(sync_dir, meta)
        itr = scandir(meta["path"])

        return itr

    def itr(self, meta, obj, obj_stats):
        config = meta["config"]
        logger = sync_logging.get_sync_logger(config["log"])

        file_regex = [re.compile(r) for r in meta['exclude_file_name']]
        dir_regex = [re.compile(r) for r in meta['exclude_directory_name']]

        full_path = os.path.abspath(obj.path)
        mode = obj.stat(follow_symlinks=False).st_mode

        try:
            if self.exclude_file_type(dir_regex, file_regex, full_path, logger, mode):
                raise ContinueException

            if not obj.is_symlink() and not bool(mode & stat.S_IRGRP):
                logger.error(
                    'physical path is not readable [{0}]'.format(full_path))
                return full_path, obj_stats

            if obj.is_dir() and not obj.is_symlink() and not obj.is_file():
                sync_dir_meta = meta.copy()
                sync_dir_meta['path'] = full_path
                sync_dir_meta['mtime'] = obj.stat(
                    follow_symlinks=False).st_mtime
                sync_dir_meta['ctime'] = obj.stat(
                    follow_symlinks=False).st_ctime
                sync_dir_meta['queue_name'] = meta["path_queue"]
                enqueue_task(sync_path, sync_dir_meta)
                raise ContinueException

        except Exception as e:
            raise ContinueException

        obj_stats['is_link'] = obj.is_symlink()
        obj_stats['is_socket'] = stat.S_ISSOCK(mode)
        obj_stats['mtime'] = obj.stat(follow_symlinks=False).st_mtime
        obj_stats['ctime'] = obj.stat(follow_symlinks=False).st_ctime
        obj_stats['size'] = obj.stat(follow_symlinks=False).st_size

        return full_path, obj_stats

    # sync_entry
    def construct_path(self, meta, path):
        #target2
        return join(meta["target"], relpath(path, start=meta["root"]))

class s3_scanner(scanner):
    def __init__(self, meta):
        super(s3_scanner, self).__init__(meta)
        self.proxy_url = meta.get('s3_proxy_url')
        self.endpoint_domain = meta.get('s3_endpoint_domain')
        self.s3_access_key = meta.get('s3_access_key')
        self.s3_secret_key = meta.get('s3_secret_key')

    # sync_path
    def instantiate(self, meta):
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

        return itr

    def itr(self, meta, obj, obj_stats):
        full_path = obj.object_name

        try:
            if obj.object_name.endswith('/'):
                return full_path, obj_stats
        except Exception as e:
            raise ContinueException

        obj_stats['is_link'] = False
        obj_stats['is_socket'] = False
        obj_stats['mtime'] = obj.last_modified.timestamp()
        obj_stats['ctime'] = obj.last_modified.timestamp()
        obj_stats['size'] = obj.size

        return full_path, obj_stats

    # sync_entry
    def construct_path(self, meta, path):
        # Strip prefix from S3 path
        prefix = meta['s3_prefix']
        reg_path = path[path.index(
            prefix) + len(prefix):].strip('/')
        # Construct S3 "logical path"
        target2 = join(meta["target"], reg_path)
        # Construct S3 "physical path" as: /bucket/objectname
        meta['path'] = '/' + join(meta["root"], path)

        return target2

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

# at bottom for circular dependency issues 
from .sync_task import sync_files, sync_dir, sync_path
