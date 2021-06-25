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


@task_prerun.connect()
def task_prerun(task_id=None, task=None, args=None, kwargs=None, **kw):
    meta = args[0]
    if meta["profile"]:
        config = meta["config"]
        profile_log = config.get("profile")
        logger = sync_logging.get_sync_logger(profile_log)
        logger.info("task_prerun", event_id=task_id, event_name=task.name, path=meta.get(
            "path"), target=meta.get("target"), hostname=task.request.hostname, index=current_process().index)


@task_postrun.connect()
def task_postrun(task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, **kw):
    meta = args[0]
    if meta["profile"]:
        config = meta["config"]
        profile_log = config.get("profile")
        logger = sync_logging.get_sync_logger(profile_log)
        logger.info("task_postrun", event_id=task_id, event_name=task.name, path=meta.get("path"), target=meta.get(
            "target"), hostname=task.request.hostname, index=current_process().index, state=state)


class IrodsTask(app.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job = sync_job.from_meta(meta)
        logger = sync_logging.get_sync_logger(config["log"])
        logger.error('failed_task', task=meta["task"], path=meta["path"], job_name=job.name(
        ), task_id=task_id, exc=exc, einfo=einfo, traceback=traceback.extract_tb(exc.__traceback__))
        job.failures_handle().incr()

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job = sync_job.from_meta(meta)
        logger = sync_logging.get_sync_logger(config["log"])
        logger.warning('retry_task', task=meta["task"], path=meta["path"], job_name=job.name(
        ), task_id=task_id, exc=exc, einfo=einfo, traceback=traceback.extract_tb(exc.__traceback__))
        job.retries_handle().incr()

    def on_success(self, retval, task_id, args, kwargs):
        meta = args[0]
        config = meta["config"]
        logger = sync_logging.get_sync_logger(config["log"])
        job_name = meta["job_name"]
        logger.info('succeeded_task', task=meta["task"], path=meta["path"],
                    job_name=job_name, task_id=task_id, retval=retval)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job = sync_job.from_meta(meta)
        logger = sync_logging.get_sync_logger(config["log"])
        logger.info('decr_job_name', task=meta["task"], path=meta["path"], job_name=job.name(
        ), task_id=task_id, retval=retval)

        done = job.tasks_handle().decr() and not job.periodic()
        if done:
            job.cleanup()

        job.dequeue_handle().rpush(task_id)

        if done:
            event_handler = custom_event_handler(meta)
            if event_handler.hasattr('post_job'):
                module = event_handler.get_module()
                module.post_job(module, logger, meta)


class RestartTask(app.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job_name = meta["job_name"]
        logger = sync_logging.get_sync_logger(config["log"])
        logger.error('failed_restart', path=meta["path"], job_name=job_name, task_id=task_id,
                     exc=exc, einfo=einfo, traceback=traceback.extract_tb(exc.__traceback__))


@app.task(base=RestartTask)
def restart(meta):
    # Start periodic job on restart_queue
    job_name = meta["job_name"]
    restart_queue = meta["restart_queue"]
    interval = meta["interval"]
    if interval is not None:
        restart.s(meta).apply_async(task_id=job_name,
                                    queue=restart_queue, countdown=interval)

    # Continue with singlepass job
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)
    try:
        event_handler = custom_event_handler(meta)
        if event_handler.hasattr('pre_job'):
            module = event_handler.get_module()
            module.pre_job(module, logger, meta)

        logger.info("***************** restart *****************")
        job = sync_job.from_meta(meta)
        if not job.periodic() or job.done():
            logger.info(
                "no tasks for this job and worker handling this task is not busy")

            job.reset()
            meta = meta.copy()
            meta["task"] = "sync_path"
            #task_queue(meta["path_queue"]).add(sync_path, meta)
            meta['queue_name'] = meta["path_queue"]
            enqueue_task(sync_path, meta)
        else:
            logger.info(
                "tasks exist for this job or worker handling this task is busy")

    except OSError as err:
        logger.warning("Warning: " + str(err),
                       traceback=traceback.extract_tb(err.__traceback__))

    except Exception as err:
        logger.error("Unexpected error: " + str(err),
                     traceback=traceback.extract_tb(err.__traceback__))
        raise


@app.task(bind=True, base=IrodsTask)
def sync_path(self, meta):
    syncer = scanner.scanner_factory(meta)
    syncer.sync_path(self, meta)


@app.task(bind=True, base=IrodsTask)
def sync_dir(self, meta):
    syncer = scanner.scanner_factory(meta)
    syncer.sync_entry(self, meta, "dir", sync_irods.sync_data_from_dir,
                      sync_irods.sync_metadata_from_dir)


@app.task(bind=True, base=IrodsTask)
def sync_files(self, _meta):
    #import here due to circular dependencies
    from .scanner import scanner_factory
    chunk = _meta["chunk"]
    meta = _meta.copy()
    for path, obj_stats in chunk.items():
        meta['path'] = path
        meta["is_empty_dir"] = obj_stats.get('is_empty_dir')
        meta["is_link"] = obj_stats.get('is_link')
        meta["is_socket"] = obj_stats.get('is_socket')
        meta["mtime"] = obj_stats.get('mtime')
        meta["ctime"] = obj_stats.get('ctime')
        meta["size"] = obj_stats.get('size')
        meta['task'] = 'sync_file'
        syncer = scanner_factory(meta)
        syncer.sync_entry(self, meta, "file", sync_irods.sync_data_from_file,
                          sync_irods.sync_metadata_from_file)


# Use the built-in version of scandir/walk if possible, otherwise
# use the scandir module version
try:
    from os import scandir
except ImportError:
    from scandir import scandir

#at bottom for circular dependency issues 
from . import scanner
