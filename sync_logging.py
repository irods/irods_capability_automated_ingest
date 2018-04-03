import sys
import logging

irods_sync_logger = "irods_sync"

def create_sync_logger(log_file):
    logger = logging.getLogger(irods_sync_logger)


    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
    if log_file is not None:
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
