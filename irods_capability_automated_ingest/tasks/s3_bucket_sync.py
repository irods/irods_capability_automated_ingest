from .. import sync_logging
from ..celery import app, RestartTask
from ..char_map_util import translate_path
from ..custom_event_handler import custom_event_handler
from ..irods import s3_bucket
from ..redis_key import sync_time_key_handle
from ..redis_utils import get_redis
from ..sync_job import sync_job
from ..utils import enqueue_task, is_unicode_encode_error_path
from .irods_task import IrodsTask

from billiard import current_process
from minio import Minio

import base64
import datetime
import os
import re
import redis_lock
import stat
import traceback


@app.task(base=RestartTask)
def s3_bucket_main_task(meta):
    # Start periodic job on restart_queue
    job_name = meta["job_name"]
    restart_queue = meta["restart_queue"]
    interval = meta["interval"]
    if interval is not None:
        restart.s(meta).apply_async(
            task_id=job_name, queue=restart_queue, countdown=interval
        )

    # Continue with singlepass job
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)
    try:
        event_handler = custom_event_handler(meta)
        if event_handler.hasattr("pre_job"):
            module = event_handler.get_module()
            module.pre_job(module, logger, meta)

        logger.info("***************** restart *****************")
        job = sync_job.from_meta(meta)
        if not job.periodic() or job.done():
            logger.info(
                "no tasks for this job and worker handling this task is not busy"
            )

            job.reset()
            meta = meta.copy()
            meta["task"] = "s3_bucket_sync_path"
            meta["queue_name"] = meta["path_queue"]
            enqueue_task(s3_bucket_sync_path, meta)
        else:
            logger.info("tasks exist for this job or worker handling this task is busy")

    except OSError as err:
        logger.warning(
            "Warning: " + str(err), traceback=traceback.extract_tb(err.__traceback__)
        )

    except Exception as err:
        logger.error(
            "Unexpected error: " + str(err),
            traceback=traceback.extract_tb(err.__traceback__),
        )
        raise


@app.task(bind=True, base=IrodsTask)
def s3_bucket_sync_path(self, meta):
    path = meta["path"]
    config = meta["config"]
    logging_config = config["log"]

    logger = sync_logging.get_sync_logger(logging_config)

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
    client = Minio(
        endpoint_domain,
        access_key=s3_access_key,
        secret_key=s3_secret_key,
        secure=s3_secure_connection,
        http_client=httpClient,
    )

    try:
        logger.info("walk dir", path=path)
        # TODO: Remove shadowing here - use a different name
        meta = meta.copy()
        meta["task"] = "s3_bucket_sync_dir"
        chunk = {}

        path_list = meta["path"].lstrip("/").split("/", 1)
        bucket_name = path_list[0]
        if len(path_list) == 1:
            prefix = ""
        else:
            prefix = path_list[1]
        meta["root"] = bucket_name
        meta["s3_prefix"] = prefix
        itr = client.list_objects(bucket_name, prefix=prefix, recursive=True)

        if meta["profile"]:
            profile_log = config.get("profile")
            profile_logger = sync_logging.get_sync_logger(profile_log)
            task_id = self.request.id

            profile_logger.info(
                "list_dir_prerun",
                event_id=task_id + ":list_dir",
                event_name="list_dir",
                hostname=self.request.hostname,
                index=current_process().index,
            )
            itr = list(itr)
            if meta["profile"]:
                profile_logger.info(
                    "list_dir_postrun",
                    event_id=task_id + ":list_dir",
                    event_name="list_dir",
                    hostname=self.request.hostname,
                    index=current_process().index,
                )

        exclude_file_name = meta["exclude_file_name"]
        exclude_directory_name = meta["exclude_directory_name"]
        file_regex = [re.compile(r) for r in exclude_file_name]
        dir_regex = [re.compile(r) for r in exclude_directory_name]

        for obj in itr:
            obj_stats = {}

            full_path = obj.object_name
            full_path = obj.object_name

            if obj.object_name.endswith("/"):
                # TODO: Not sure what this means -- skip it?
                # chunk[full_path] = {}
                continue

            # add object stat dict to the chunk dict
            obj_stats = {
                "is_link": False,
                "is_socket": False,
                "mtime": obj.last_modified.timestamp(),
                "ctime": obj.last_modified.timestamp(),
                "size": obj.size,
            }
            chunk[full_path] = obj_stats

            # Launch async job when enough objects are ready to be sync'd
            files_per_task = meta.get("files_per_task")
            if len(chunk) >= files_per_task:
                sync_files_meta = meta.copy()
                sync_files_meta["chunk"] = chunk
                sync_files_meta["queue_name"] = meta["file_queue"]
                enqueue_task(s3_bucket_sync_files, sync_files_meta)
                chunk.clear()

        if len(chunk) > 0:
            sync_files_meta = meta.copy()
            sync_files_meta["chunk"] = chunk
            sync_files_meta["queue_name"] = meta["file_queue"]
            enqueue_task(s3_bucket_sync_files, sync_files_meta)
            chunk.clear()

    except Exception as err:
        event_handler = custom_event_handler(meta)
        retry_countdown = event_handler.delay(self.request.retries + 1)
        max_retries = event_handler.max_retries()
        raise self.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)


@app.task(bind=True, base=IrodsTask)
def s3_bucket_sync_dir(self, meta_input):
    meta = meta_input.copy()
    meta["entry_type"] = "dir"
    s3_bucket_sync_entry(
        self, meta, s3_bucket.sync_data_from_dir, s3_bucket.sync_metadata_from_dir
    )


@app.task(bind=True, base=IrodsTask)
def s3_bucket_sync_files(self, meta_input):
    meta = meta_input.copy()
    meta["entry_type"] = "file"
    meta["task"] = "sync_file"
    for path, obj_stats in meta["chunk"].items():
        meta["path"] = path
        meta["is_empty_dir"] = obj_stats.get("is_empty_dir")
        meta["is_link"] = obj_stats.get("is_link")
        meta["is_socket"] = obj_stats.get("is_socket")
        meta["mtime"] = obj_stats.get("mtime")
        meta["ctime"] = obj_stats.get("ctime")
        meta["size"] = obj_stats.get("size")
        s3_bucket_sync_entry(
            self, meta, s3_bucket.sync_data_from_file, s3_bucket.sync_metadata_from_file
        )


def s3_bucket_sync_entry(self, meta_input, datafunc, metafunc):
    meta = meta_input.copy()

    path = meta["path"]
    target = meta["target"]
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)

    entry_type = meta["entry_type"]

    event_handler = custom_event_handler(meta)
    max_retries = event_handler.max_retries()

    lock = None

    logger.info("synchronizing " + entry_type + ". path = " + path)

    character_map = getattr(event_handler.get_module(), "character_map", None)
    path_requires_UnicodeEncodeError_handling = is_unicode_encode_error_path(path)

    # TODO: Pull out this logic into some functions
    if path_requires_UnicodeEncodeError_handling or character_map is not None:
        abspath = os.path.abspath(path)
        utf8_escaped_abspath = abspath.encode("utf8", "surrogateescape")
        b64_path_str = base64.b64encode(utf8_escaped_abspath)

    if path_requires_UnicodeEncodeError_handling:
        path = os.path.dirname(abspath)
        unicode_error_filename = "irods_UnicodeEncodeError_" + str(
            b64_path_str.decode("utf8")
        )
        logger.warning(
            "s3_bucket_sync_entry raised UnicodeEncodeError while syncing path:"
            + str(utf8_escaped_abspath)
        )
        meta["path"] = path
        meta["b64_path_str"] = b64_path_str
        meta["b64_reason"] = "UnicodeEncodeError"
        meta["unicode_error_filename"] = unicode_error_filename
        sync_key = str(b64_path_str.decode("utf8")) + ":" + target
    else:
        sync_key = path + ":" + target

    try:
        r = get_redis(config)
        lock = redis_lock.Lock(r, "sync_" + entry_type + ":" + sync_key)
        lock.acquire()

        sync_time_handle = sync_time_key_handle(r, sync_key)
        ignore_redis_cache = meta.get("ignore_cache", False)
        sync_time = None if ignore_redis_cache else sync_time_handle.get_value()

        mtime = meta["mtime"]
        ctime = meta["ctime"]

        if sync_time is not None and mtime < sync_time and ctime < sync_time:
            logger.info(
                "succeeded_" + entry_type + "_has_not_changed",
                task=meta["task"],
                path=path,
            )
            return

        t = datetime.datetime.now().timestamp()
        logger.info(
            "synchronizing " + entry_type, path=path, t0=sync_time, t=t, ctime=ctime
        )
        meta2 = meta.copy()
        if path == meta["root"]:
            if path_requires_UnicodeEncodeError_handling:
                # TODO(#250): This may not work on Windows...
                target2 = os.path.join(target, meta["unicode_error_filename"])
            else:
                target2 = target
        else:
            # Strip prefix from S3 path
            prefix = meta["s3_prefix"]
            reg_path = path[path.index(prefix) + len(prefix) :].strip("/")
            # Construct S3 "logical path"
            target2 = "/".join([meta["target"], reg_path])
            # Construct S3 "physical path" as: /bucket/objectname
            meta["path"] = f"/{meta['root']}/{path}"

        # If the event handler has a character_map function, it should have returned a
        # structure (either a dict or a list/tuple of key-value tuples) to be used for
        # instantiating a collections.OrderedDict object. This object will dictate how
        # the logical path's characters are remapped.  The re-mapping is performed
        # independently for each path element of the collection hierarchy.

        if not path_requires_UnicodeEncodeError_handling and character_map is not None:
            translated_path = translate_path(target2, character_map())
            # arrange for AVU to be attached only when logical name changes
            if translated_path != target2:
                target2 = translated_path
                meta2["b64_reason"] = "character_map"
                meta2["b64_path_str_charmap"] = b64_path_str

        meta2["target"] = target2

        if sync_time is None or mtime >= sync_time:
            datafunc(event_handler.get_module(), meta2, logger, True)
            logger.info("succeeded", task=meta["task"], path=path)
        else:
            metafunc(event_handler.get_module(), meta2, logger)
            logger.info("succeeded_metadata_only", task=meta["task"], path=path)
        sync_time_handle.set_value(str(t))
    except Exception as err:
        event_handler = custom_event_handler(meta)
        retry_countdown = event_handler.delay(self.request.retries + 1)
        raise self.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)
    finally:
        if lock is not None:
            lock.release()
