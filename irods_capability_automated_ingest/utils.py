from . import sync_logging
from .sync_job import sync_job
from .custom_event_handler import custom_event_handler
from uuid import uuid1

from enum import Enum
import os
import stat


class Operation(Enum):
    REGISTER_SYNC = 0
    REGISTER_AS_REPLICA_SYNC = 1
    PUT = 2
    PUT_SYNC = 3
    PUT_APPEND = 4
    NO_OP = 5


class DeleteMode(Enum):
    DO_NOT_DELETE = 0
    UNREGISTER = 1
    TRASH = 2
    NO_TRASH = 3


def delete_mode_is_compatible_with_operation(delete_mode, operation):
    operation_to_acceptable_delete_modes = {
        Operation.NO_OP: [
            DeleteMode.DO_NOT_DELETE,
        ],
        Operation.REGISTER_SYNC: [
            DeleteMode.DO_NOT_DELETE,
            DeleteMode.UNREGISTER,
        ],
        Operation.REGISTER_AS_REPLICA_SYNC: [
            DeleteMode.DO_NOT_DELETE,
            DeleteMode.UNREGISTER,
        ],
        Operation.PUT: [
            DeleteMode.DO_NOT_DELETE,
        ],
        Operation.PUT_SYNC: [
            DeleteMode.DO_NOT_DELETE,
            DeleteMode.TRASH,
            DeleteMode.NO_TRASH,
        ],
        Operation.PUT_APPEND: [
            DeleteMode.DO_NOT_DELETE,
            DeleteMode.TRASH,
            DeleteMode.NO_TRASH,
        ],
    }
    return delete_mode in operation_to_acceptable_delete_modes.get(operation, [])


def enqueue_task(task, meta):
    logger = sync_logging.get_sync_logger(meta["config"]["log"])
    job = sync_job.from_meta(meta)
    if job.stop_handle().get_value() is None:
        logger.info(
            "incr_job_name", task=meta["task"], path=meta["path"], job_name=job.name()
        )
        job.tasks_handle().incr()
        task_id = str(uuid1())
        timeout = custom_event_handler(meta).timeout()
        job.count_handle().rpush(task_id)
        task.s(meta).apply_async(
            queue=meta["queue_name"], task_id=task_id, soft_time_limit=timeout
        )
    else:
        # A job by this name is currently being stopped
        logger.info(
            "async_job_name_stopping",
            task=meta["task"],
            path=meta["path"],
            job_name=job.name(),
        )


# Attempt to encode full physical path on local filesystem
# Special handling required for non-encodable strings which raise UnicodeEncodeError
def is_unicode_encode_error_path(path):
    try:
        _ = path.encode("utf8")
    except UnicodeEncodeError:
        return True
    return False
