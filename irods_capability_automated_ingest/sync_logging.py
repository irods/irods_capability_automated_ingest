import structlog
import logging
import logging.handlers
from structlog import wrap_logger
import datetime
import time
import sys

irods_sync_logger = "irods_sync"


def timestamper(logger, log_method, event_dict):
    utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
    utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
    event_dict["@timestamp"] = datetime.datetime.now().replace(tzinfo=datetime.timezone(offset=utc_offset)).isoformat()
    return event_dict


logger_map = {}


def create_sync_logger(logging_config):
    log_file = logging_config["filename"]
    when = logging_config["when"]
    interval = logging_config["interval"]
    level = logging_config["level"]

    logger = logging.getLogger(irods_sync_logger + "/" + get_sync_logger_key(logging_config))
    logger.propagate = False

    # logger = get_task_logger(irods_sync_logger)

    if level is not None:
        logger.setLevel(logging.getLevelName(level))

    if log_file is not None:
        if when is not None:
            handler = logging.handlers.TimedRotatingFileHandler(log_file, when=when, interval=interval)
        else:
            handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler(sys.stdout)
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


def get_sync_logger(logging_config):
    key = get_sync_logger_key(logging_config)
    logger = logger_map.get(key)
    if logger is None:
        logger = create_sync_logger(logging_config)
        logger_map[key] = logger

    return logger


def get_sync_logger_key(logging_config):
    filename = logging_config["filename"]
    if filename is None:
        filename = ""
    level = logging_config["level"]
    if level is None:
        level = ""
    return filename + "/" + level
