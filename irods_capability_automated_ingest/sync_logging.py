import sys
import logging
import logging.handlers

irods_sync_logger = "irods_sync"

def create_sync_logger(logging_config):
    log_file = logging_config["log"].get("filename")
    when = logging_config["log"].get("when")
    interval = logging_config["log"].get("interval")
    level = logging_config["log"].get("level")

    logger = logging.getLogger(irods_sync_logger)

    if level is not None:
        logger.setLevel(logging.getLevelName(level))

    formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
    if log_file is not None:
        if when is not None:
            handler2 = logging.handlers.TimedRotatingFileHandler(log_file, when=when, interval=interval)
        else:
            handler2 = logging.FileHandler(log_file)
        handler2.setFormatter(formatter)
        logger.addHandler(handler2)
    else:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def get_sync_logger():
    return logging.getLogger(irods_sync_logger)
