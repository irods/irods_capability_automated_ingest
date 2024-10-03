from . import redis_key
from .celery import app
from .redis_utils import get_redis

import datetime
import json
import os
import progressbar
import time


def add_stopped_job(redis_handle, stopped_job_dict):
    """Add the sync_job dict to the JSON array of stopped jobs tracked in the Redis database."""
    stopped_jobs_handle = redis_key.stopped_jobs_key_handle(redis_handle)
    stopped_jobs_value = stopped_jobs_handle.get_value()
    if stopped_jobs_value is None:
        stopped_jobs_list = []
    else:
        stopped_jobs_list = json.loads(stopped_jobs_value.decode("utf-8"))
    stopped_jobs_list.append(stopped_job_dict)
    # TODO(#297): Is it really the caller's responsibility to dump to a string?
    # stopped_jobs_handle is a json_redis_key_handle, so it ought to handle this for us...
    stopped_jobs_handle.set_value(json.dumps(stopped_jobs_list))


def get_stopped_jobs_list(redis_handle):
    """Get the JSON array of stopped jobs tracked in the Redis database."""
    stopped_jobs_value = redis_key.stopped_jobs_key_handle(redis_handle).get_value()
    if stopped_jobs_value is None:
        return []
    return json.loads(stopped_jobs_value.decode("utf-8"))


class sync_job(object):
    def __init__(self, job_name, redis_handle):
        self.job_name = job_name
        self.r = redis_handle

    @classmethod
    def from_meta(cls, meta):
        return cls(meta["job_name"], get_redis(meta["config"]))

    def name(self):
        return self.job_name

    def count_handle(self):
        return redis_key.count_key_handle(self.r, self.job_name)

    def dequeue_handle(self):
        return redis_key.dequeue_key_handle(self.r, self.job_name)

    def tasks_handle(self):
        return redis_key.tasks_key_handle(self.r, self.job_name)

    def failures_handle(self):
        return redis_key.failures_key_handle(self.r, self.job_name)

    def retries_handle(self):
        return redis_key.retries_key_handle(self.r, self.job_name)

    def stop_handle(self):
        return redis_key.stop_key_handle(self.r, self.job_name)

    def cleanup_handle(self):
        return redis_key.cleanup_key_handle(self.r, self.job_name)

    def done(self):
        task_count = self.tasks_handle().get_value()
        return task_count is None or task_count == 0

    def periodic(self):
        periodic_list = self.r.lrange("periodic", 0, -1)
        return self.job_name.encode("utf-8") in periodic_list

    def cleanup(self):
        # hdlr = get_with_key(r, cleanup_key, job_name, lambda bs: json.loads(bs.decode("utf-8")))
        cleanup_list = self.cleanup_handle().get_value()
        if cleanup_list is not None:
            file_list = json.loads(cleanup_list.decode("utf-8"))
            for f in file_list:
                os.remove(f)

        if self.periodic():
            self.r.lrem("periodic", 1, self.job_name)
        else:
            self.r.lrem("singlepass", 1, self.job_name)

        self.cleanup_handle().reset()

    def reset(self):
        self.count_handle().reset()
        self.dequeue_handle().reset()
        self.tasks_handle().reset()
        self.failures_handle().reset()
        self.retries_handle().reset()
        self.start_time_handle().reset()

    def interrupt(self, cli=True, terminate=True):
        self.stop_handle().set_value("")
        queued_tasks = list(
            map(lambda x: x.decode("utf-8"), self.count_handle().lrange(0, -1))
        )
        dequeued_tasks = set(
            map(lambda x: x.decode("utf-8"), self.dequeue_handle().lrange(0, -1))
        )

        tasks = [item for item in queued_tasks if item not in dequeued_tasks]
        if cli:
            tasks = progressbar.progressbar(tasks, max_value=len(tasks))

        # stop active tasks for this job
        for task in tasks:
            app.control.revoke(task, terminate=terminate)

        # stop restart job
        app.control.revoke(self.job_name)
        self.stop_handle().reset()

    def start_time_handle(self):
        return redis_key.float_redis_key_handle(
            self.r, "irods_ingest_job_start_time", self.job_name
        )

    def stop(self):
        add_stopped_job(self.r, self.asdict())
        self.interrupt()
        self.cleanup()
        self.reset()

    def stopped(self):
        stopped_jobs_list = get_stopped_jobs_list(self.r)
        for job in stopped_jobs_list:
            if self.job_name == job["job_name"]:
                return True
        return False

    def asdict(self):
        start_time = self.start_time_handle().get_value() or 0
        formatted_start_time = datetime.datetime.fromtimestamp(
            start_time, tz=datetime.timezone.utc
        ).isoformat(timespec="milliseconds")
        elapsed_time = time.time() - start_time if start_time else 0
        elapsed_time_str = str(datetime.timedelta(milliseconds=elapsed_time * 1000))
        tasks = int(self.tasks_handle().get_value() or 0)
        total = self.count_handle().llen()
        failures = int(self.failures_handle().get_value() or 0)
        retries = int(self.retries_handle().get_value() or 0)
        return {
            "job_name": self.job_name,
            "total_tasks": total,
            "remaining_tasks": tasks,
            "failed_tasks": failures,
            "retried_tasks": retries,
            "elapsed_time": elapsed_time_str,
            "start_time": formatted_start_time,
        }
