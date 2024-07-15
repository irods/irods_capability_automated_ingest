import re
import base64
from minio import Minio
from .sync_utils import get_redis
from .redis_key import sync_time_key_handle
import redis_lock
from datetime import datetime
from os.path import join, getmtime, relpath, getctime
import os
import stat
from . import sync_logging, sync_irods
from .custom_event_handler import custom_event_handler
from .sync_job import sync_job
from .sync_utils import app
from .utils import enqueue_task
from .task_queue import task_queue
import traceback
from celery.signals import task_prerun, task_postrun
from billiard import current_process
from .char_map_util import translate_path


class ContinueException(Exception):
    pass


@task_prerun.connect()
def task_prerun(task_id=None, task=None, args=None, kwargs=None, **kw):
    meta = args[0]
    if meta["profile"]:
        config = meta["config"]
        profile_log = config.get("profile")
        logger = sync_logging.get_sync_logger(profile_log)
        logger.info(
            "task_prerun",
            event_id=task_id,
            event_name=task.name,
            path=meta.get("path"),
            target=meta.get("target"),
            hostname=task.request.hostname,
            index=current_process().index,
        )


@task_postrun.connect()
def task_postrun(
    task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, **kw
):
    meta = args[0]
    if meta["profile"]:
        config = meta["config"]
        profile_log = config.get("profile")
        logger = sync_logging.get_sync_logger(profile_log)
        logger.info(
            "task_postrun",
            event_id=task_id,
            event_name=task.name,
            path=meta.get("path"),
            target=meta.get("target"),
            hostname=task.request.hostname,
            index=current_process().index,
            state=state,
        )


class IrodsTask(app.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job = sync_job.from_meta(meta)
        logger = sync_logging.get_sync_logger(config["log"])
        logger.error(
            "failed_task",
            task=meta["task"],
            path=meta["path"],
            job_name=job.name(),
            task_id=task_id,
            exc=exc,
            einfo=einfo,
            traceback=traceback.extract_tb(exc.__traceback__),
        )
        job.failures_handle().incr()

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job = sync_job.from_meta(meta)
        logger = sync_logging.get_sync_logger(config["log"])
        logger.warning(
            "retry_task",
            task=meta["task"],
            path=meta["path"],
            job_name=job.name(),
            task_id=task_id,
            exc=exc,
            einfo=einfo,
            traceback=traceback.extract_tb(exc.__traceback__),
        )
        job.retries_handle().incr()

    def on_success(self, retval, task_id, args, kwargs):
        meta = args[0]
        config = meta["config"]
        logger = sync_logging.get_sync_logger(config["log"])
        job_name = meta["job_name"]
        logger.info(
            "succeeded_task",
            task=meta["task"],
            path=meta["path"],
            job_name=job_name,
            task_id=task_id,
            retval=retval,
        )

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job = sync_job.from_meta(meta)
        logger = sync_logging.get_sync_logger(config["log"])
        logger.info(
            "decr_job_name",
            task=meta["task"],
            path=meta["path"],
            job_name=job.name(),
            task_id=task_id,
            retval=retval,
        )

        done = job.tasks_handle().decr() == 0 and not job.periodic()
        if done:
            job.cleanup()

        job.dequeue_handle().rpush(task_id)

        if done:
            event_handler = custom_event_handler(meta)
            if event_handler.hasattr("post_job"):
                module = event_handler.get_module()
                module.post_job(module, logger, meta)


class RestartTask(app.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job_name = meta["job_name"]
        logger = sync_logging.get_sync_logger(config["log"])
        logger.error(
            "failed_restart",
            path=meta["path"],
            job_name=job_name,
            task_id=task_id,
            exc=exc,
            einfo=einfo,
            traceback=traceback.extract_tb(exc.__traceback__),
        )


@app.task(base=RestartTask)
def restart(meta):
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
            meta["task"] = "sync_path"
            meta["queue_name"] = meta["path_queue"]
            enqueue_task(sync_path, meta)
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
def sync_path(self, meta):
    scanner_instance = scanner.scanner_factory(meta)
    path = meta["path"]
    config = meta["config"]
    logging_config = config["log"]
    exclude_file_name = meta["exclude_file_name"]
    exclude_directory_name = meta["exclude_directory_name"]

    logger = sync_logging.get_sync_logger(logging_config)

    file_regex = [re.compile(r) for r in exclude_file_name]
    dir_regex = [re.compile(r) for r in exclude_directory_name]

    try:
        logger.info("walk dir", path=path)
        meta = meta.copy()
        meta["task"] = "sync_dir"
        chunk = {}

        itr = scanner_instance.instantiate(meta)

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

        for obj in itr:
            obj_stats = {}

            try:
                full_path, obj_stats = scanner_instance.itr(meta, obj, obj_stats)
            except ContinueException:
                continue

            # add object stat dict to the chunk dict
            chunk[full_path] = obj_stats

            # Launch async job when enough objects are ready to be sync'd
            files_per_task = meta.get("files_per_task")
            if len(chunk) >= files_per_task:
                sync_files_meta = meta.copy()
                sync_files_meta["chunk"] = chunk
                sync_files_meta["queue_name"] = meta["file_queue"]
                enqueue_task(sync_files, sync_files_meta)
                chunk.clear()

        if len(chunk) > 0:
            sync_files_meta = meta.copy()
            sync_files_meta["chunk"] = chunk
            sync_files_meta["queue_name"] = meta["file_queue"]
            enqueue_task(sync_files, sync_files_meta)
            chunk.clear()

    except Exception as err:
        event_handler = custom_event_handler(meta)
        retry_countdown = event_handler.delay(self.request.retries + 1)
        max_retries = event_handler.max_retries()
        raise self.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)


def sync_entry(self, meta, cls, scanner_instance, datafunc, metafunc):
    path = meta["path"]
    target = meta["target"]
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)

    event_handler = custom_event_handler(meta)
    max_retries = event_handler.max_retries()

    lock = None

    logger.info("synchronizing " + cls + ". path = " + path)

    char_map_func = getattr(event_handler.get_module(), "character_map", None)
    unicode_error = scanner.is_unicode_encode_error_path(path)

    if (
        unicode_error or char_map_func
    ):  # or char_map_func: # compute things we could need for either case
        abspath = os.path.abspath(path)
        utf8_escaped_abspath = abspath.encode("utf8", "surrogateescape")
        b64_path_str = base64.b64encode(utf8_escaped_abspath)

    if unicode_error:
        path = os.path.dirname(abspath)
        unicode_error_filename = "irods_UnicodeEncodeError_" + str(
            b64_path_str.decode("utf8")
        )
        logger.warning(
            "sync_entry raised UnicodeEncodeError while syncing path:"
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
        lock = redis_lock.Lock(r, "sync_" + cls + ":" + sync_key)
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
            logger.info(
                "succeeded_" + cls + "_has_not_changed", task=meta["task"], path=path
            )
        else:
            t = datetime.now().timestamp()
            logger.info(
                "synchronizing " + cls, path=path, t0=sync_time, t=t, ctime=ctime
            )
            meta2 = meta.copy()
            if path == meta["root"]:
                if "unicode_error_filename" in meta:
                    target2 = join(target, meta["unicode_error_filename"])
                else:
                    target2 = target
            else:
                target2 = scanner_instance.construct_path(meta2, path)

            # If the event handler has a character_map function, it should have returned a
            # structure (either a dict or a list/tuple of key-value tuples) to be used for
            # instantiating a collections.OrderedDict object. This object will dictate how
            # the logical path's characters are remapped.  The re-mapping is performed
            # independently for each path element of the collection hierarchy.

            if "unicode_error_filename" not in meta:
                if char_map_func:
                    translated_path = translate_path(target2, char_map_func())
                    # arrange for AVU to be attached only when logical name changes
                    if translated_path != target2:
                        target2 = translated_path
                        meta2["b64_reason"] = "character_map"
                        meta2["b64_path_str_charmap"] = b64_path_str

            meta2["target"] = target2

            if sync_time is None or mtime >= sync_time:
                datafunc(event_handler.get_module(), meta2, logger, scanner_instance, True)
                logger.info("succeeded", task=meta["task"], path=path)
            else:
                metafunc(event_handler.get_module(), meta2, logger, scanner_instance)
                logger.info("succeeded_metadata_only", task=meta["task"], path=path)
            sync_time_handle.set_value(str(t))
    except Exception as err:
        event_handler = custom_event_handler(meta)
        retry_countdown = event_handler.delay(self.request.retries + 1)
        raise self.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)
    finally:
        if lock is not None:
            lock.release()


@app.task(bind=True, base=IrodsTask)
def sync_dir(self, meta):
    scanner_instance = scanner.scanner_factory(meta)
    sync_entry(
        self,
        meta,
        "dir",
        scanner_instance,
        sync_irods.sync_data_from_dir,
        sync_irods.sync_metadata_from_dir,
    )


@app.task(bind=True, base=IrodsTask)
def sync_files(self, _meta):
    chunk = _meta["chunk"]
    meta = _meta.copy()
    scanner_instance = scanner.scanner_factory(meta)
    for path, obj_stats in chunk.items():
        meta["path"] = path
        meta["is_empty_dir"] = obj_stats.get("is_empty_dir")
        meta["is_link"] = obj_stats.get("is_link")
        meta["is_socket"] = obj_stats.get("is_socket")
        meta["mtime"] = obj_stats.get("mtime")
        meta["ctime"] = obj_stats.get("ctime")
        meta["size"] = obj_stats.get("size")
        meta["task"] = "sync_file"
        sync_entry(
            self,
            meta,
            "file",
            scanner_instance,
            sync_irods.sync_data_from_file,
            sync_irods.sync_metadata_from_file,
        )


# Use the built-in version of scandir/walk if possible, otherwise
# use the scandir module version
try:
    from os import scandir
except ImportError:
    from scandir import scandir

# at bottom for circular dependency issues
from . import scanner
