from . import custom_event_handler, sync_logging

from celery import Celery
from celery.signals import task_prerun, task_postrun

import traceback

app = Celery("irods_capability_automated_ingest")

app.conf.update(
    include=[
        "irods_capability_automated_ingest.tasks.delete_tasks",
        "irods_capability_automated_ingest.tasks.filesystem_tasks",
        "irods_capability_automated_ingest.tasks.s3_bucket_tasks",
    ]
)


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
