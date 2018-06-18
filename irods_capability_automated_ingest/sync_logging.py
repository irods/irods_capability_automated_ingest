import sys
import structlog
import logging
import logging.handlers
from structlog import wrap_logger
import datetime
import time
from celery.utils.log import get_task_logger

irods_sync_logger = "irods_sync"

def timestamper(logger, log_method, event_dict):
    utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
    utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
    event_dict["@timestamp"] = datetime.datetime.now().replace(tzinfo=datetime.timezone(offset=utc_offset)).isoformat()
    return event_dict

def create_sync_logger(logging_config):
    log_file = logging_config.get("filename")
    when = logging_config.get("when")
    interval = logging_config.get("interval")
    level = logging_config.get("level")

    logger = get_task_logger(irods_sync_logger)
    if level is not None:
        logger.setLevel(logging.getLevelName(level))

    if log_file is not None:
        if when is not None:
            handler = logging.handlers.TimedRotatingFileHandler(log_file, when=when, interval=interval)
        else:
            handler = logging.FileHandler(log_file)
    # else:
    #     handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(handler)

    return wrap_logger(
        logger,
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            timestamper,
            structlog.processors.JSONRenderer()
        ]
    )


def get_sync_logger():
    logger = get_task_logger(irods_sync_logger)

    return wrap_logger(
        logger,
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            timestamper,
            structlog.processors.JSONRenderer()
        ]
    )

