from enum import Enum
import time
import traceback

class Operation(Enum):
    REGISTER_SYNC = 0
    REGISTER_AS_REPLICA_SYNC = 1
    PUT = 2
    PUT_SYNC = 3
    PUT_APPEND = 4
    NO_OP = 5


MAX_RETRIES = 10


def retry(logger, func, *args, max_retries=MAX_RETRIES):
    retries = 0
    while retries <= max_retries:
        try:
            res = func(*args)
            return res
        except Exception as err:
            retries += 1

            logger.info('Retrying. retries=' + str(retries), max_retries=max_retries, func=func, args=args, err=err, stacktrace=traceback.extract_tb(err.__traceback__))
            time.sleep(1)
    raise RuntimeError("max retries")
