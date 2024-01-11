from . import sync_logging
from .sync_job import sync_job
from .custom_event_handler import custom_event_handler
from uuid import uuid1


class task_queue(object):
    def __init__(self, name):
        self.name = name

    def name(self):
        return self.name

    def add(self, task, meta):
        logger = sync_logging.get_sync_logger(meta["config"]["log"])
        job = sync_job.from_meta(meta)
        if job.stop_handle().get_value() is None:
            logger.info(
                "incr_job_name",
                task=meta["task"],
                path=meta["path"],
                job_name=job.name(),
            )
            job.tasks_handle().incr()
            task_id = str(uuid1())
            timeout = custom_event_handler(meta).timeout()
            job.count_handle().rpush(task_id)
            task.s(meta).apply_async(
                queue=self.name(), task_id=task_id, soft_time_limit=timeout
            )
        else:
            # A job by this name is currently being stopped
            logger.info(
                "async_job_name_stopping",
                task=meta["task"],
                path=meta["path"],
                job_name=job.name(),
            )
