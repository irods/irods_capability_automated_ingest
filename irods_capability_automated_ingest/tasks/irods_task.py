from .. import custom_event_handler, sync_logging
from ..celery import app
from ..sync_job import sync_job

import traceback


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
            event_handler = custom_event_handler.custom_event_handler(meta)
            if event_handler.hasattr("post_job"):
                module = event_handler.get_module()
                module.post_job(module, logger, meta)
