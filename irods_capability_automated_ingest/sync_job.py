from . import redis_key
from .sync_utils import get_redis, app
import json
import os
import progressbar


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
