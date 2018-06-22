from enum import Enum
import time

class Operation(Enum):
    REGISTER_SYNC = 0
    REGISTER_AS_REPLICA_SYNC = 1
    PUT = 2
    PUT_SYNC = 3
    PUT_APPEND = 4
    NO_OP = 5


MAX_RETRIES = 10


def retry(logger, task, func, max_retries=MAX_RETRIES):
    retries = 0
    while retries <= max_retries:
        try:
            res = func()
            return res
        except Exception as err:
            retries += 1
            logger.log('failed_' + task, err=err)
            time.sleep(1)
    raise RuntimeError("max retries: " + task)
