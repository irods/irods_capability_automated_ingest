import json
from minio import Minio
import time
import traceback

MAX_RETRIES = 10


# TODO: Consider compression/hashing of key_category and identifier
class redis_key_handle(object):
    # def __init__(self, logger, redis_handle, key_category, identifier, delimiter=':/'):
    def __init__(self, redis_handle, key_category, identifier, delimiter=":/"):
        # self.logger = logger
        self.redis_handle = redis_handle
        self.category = key_category
        self.identifier = identifier
        self.delimiter = delimiter
        # TODO: Hard-coded value from .utils

    def retry(self, func, *args, max_retries=MAX_RETRIES):
        retries = 0
        while retries <= max_retries:
            try:
                res = func(*args)
                return res
            except Exception as err:
                retries += 1

                # logger.info('Retrying. retries=' + str(retries), max_retries=max_retries, func=func, args=args, err=err, stacktrace=traceback.extract_tb(err.__traceback__))
                time.sleep(1)
        raise RuntimeError("max retries")

    def get_key(self):
        return str(self.category + self.delimiter + self.identifier)

    def get_value(self):
        if self.get_key() is None:
            return None
        return self.retry(self.redis_handle.get, self.get_key())

    def set_value(self, value):
        self.retry(self.redis_handle.set, self.get_key(), value)

    def reset(self):
        self.retry(self.redis_handle.delete, self.get_key())


class incremental_redis_key_handle(redis_key_handle):
    def __init__(self, redis_handle, key_category, identifier, delimiter=":/"):
        super().__init__(redis_handle, key_category, identifier, delimiter)

    def get_value(self):
        val = super().get_value()
        if val is None:
            return val
        return int(val)

    def incrby(self, amount=1):
        self.retry(self.redis_handle.incrby, self.get_key(), amount)

    def incr(self):
        self.retry(self.redis_handle.incr, self.get_key())

    def decrby(self, amount=1):
        self.retry(self.redis_handle.decrby, self.get_key(), amount)

    def decr(self):
        return self.retry(self.redis_handle.decr, self.get_key())


class json_redis_key_handle(redis_key_handle):
    def __init__(self, redis_handle, key_category, identifier, delimiter=":/"):
        super().__init__(redis_handle, key_category, identifier, delimiter)

    # def get_value(self):
    # return json.loads(self.retry(self.redis_handle.get, self.get_key().decode("utf-8")))


class list_redis_key_handle(redis_key_handle):
    def __init__(self, redis_handle, key_category, identifier, delimiter=":/"):
        super().__init__(redis_handle, key_category, identifier, delimiter)

    def get_value(self):
        val = super().get_value()
        if val is None:
            return val
        return list(val)

    def rpush(self, value):
        self.retry(self.redis_handle.rpush, self.get_key(), value)

    def lrange(self, start, end):
        return self.retry(self.redis_handle.lrange, self.get_key(), start, end)

    def llen(self):
        return self.retry(self.redis_handle.llen, self.get_key())


# TODO: python metaclasses - see PRC
# sync_time_key - float with last time particular path was sync'd
class sync_time_key_handle(redis_key_handle):
    def __init__(self, redis_handle, path):
        super().__init__(redis_handle, "sync_time", path)

    def get_value(self):
        val = super().get_value()
        if val is None:
            return val
        return float(val)


# cleanup_key - JSON object with list of event_handlers that need to be cleaned up
class cleanup_key_handle(json_redis_key_handle):
    def __init__(self, redis_handle, job_name):
        super().__init__(redis_handle, "cleanup", job_name)


# stop_key - value:empty string (job_name needs to be stopped)
class stop_key_handle(redis_key_handle):
    def __init__(self, redis_handle, job_name_to_stop):
        super().__init__(redis_handle, "stop", job_name_to_stop)

    def get_value(self):
        val = super().get_value()
        if val is None:
            return val
        return str(val)


# tasks_key - value:int task count for job name
class tasks_key_handle(incremental_redis_key_handle):
    def __init__(self, redis_handle, job_name):
        super().__init__(redis_handle, "tasks", job_name)


# count_key - value:list of task_ids for job name
class count_key_handle(list_redis_key_handle):
    def __init__(self, redis_handle, job_name):
        super().__init__(redis_handle, "count", job_name)


# dequeue_key - value:list of tasks for a particular job_name
# TODO: What is the difference between this list and the set of stop_keys?
class dequeue_key_handle(list_redis_key_handle):
    def __init__(self, redis_handle, job_name):
        super().__init__(redis_handle, "dequeue", job_name)


# failures_key - value:int with count of failed tasks
class failures_key_handle(incremental_redis_key_handle):
    def __init__(self, redis_handle, job_name):
        super().__init__(redis_handle, "failures", job_name)


# retries_key - value:int number of retries attempted for job_name
class retries_key_handle(incremental_redis_key_handle):
    def __init__(self, redis_handle, job_name):
        super().__init__(redis_handle, "retries", job_name)
