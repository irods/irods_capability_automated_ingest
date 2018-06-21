from os import listdir
import os
from os.path import isfile, join, getmtime, realpath, relpath, getctime
from datetime import datetime
import redis_lock
from . import sync_logging, sync_irods
from .sync_utils import get_redis, app, get_with_key, get_max_retries, tasks_key, set_with_key, decr_with_key, incr_with_key, reset_with_key, cleanup_key, sync_time_key, get_timeout, failures_key, retries_key, get_delay
from uuid import uuid1
import time


class IrodsTask(app.Task):

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job_name = meta["job_name"]
        logger = sync_logging.get_sync_logger(config["log"])
        r = get_redis(config)
        logger.error('failed_task', task=meta["task"], path=meta["path"], job_name=job_name, task_id=task_id, exc=exc, einfo=einfo)
        incr_with_key(r, failures_key, job_name)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job_name = meta["job_name"]
        logger = sync_logging.get_sync_logger(config["log"])
        r = get_redis(config)
        logger.warn('retry_task', task=meta["task"], path=meta["path"], job_name=job_name, task_id=task_id, exc=exc, einfo=einfo)
        incr_with_key(r, retries_key, job_name)

    def on_success(self, retval, task_id, args, kwargs):
        meta = args[0]
        config = meta["config"]
        logger = sync_logging.get_sync_logger(config["log"])
        job_name = meta["job_name"]
        logger.info('succeeded_task', task=meta["task"], path=meta["path"], job_name=job_name, task_id=task_id, retval=retval)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job_name = meta["job_name"]
        logger = sync_logging.get_sync_logger(config["log"])
        r = get_redis(config)
        logger.info('decr_job_name', task=meta["task"], path=meta["path"], job_name=job_name, task_id=task_id, retval=retval)
        decr_with_key(r, tasks_key, job_name)


def async(r, logger, task, meta, queue):
    job_name = meta["job_name"]
    logger.info('incr_job_name', task=meta["task"], path=meta["path"], job_name=job_name)
    incr_with_key(r, tasks_key, job_name)
    task.s(meta).apply_async(queue=queue,task_id=task.name+":"+meta["path"]+":"+meta["target"]+":"+str(time.time()))


def done(r, job_name):
    ntasks = get_with_key(r, tasks_key, job_name, int)
    return ntasks is None or ntasks == 0


def cleanup(r, job_name):
    hdlr = get_with_key(r, cleanup_key, job_name, lambda x: x)
    if hdlr is not None:
        os.remove(hdlr.decode("utf-8"))
        reset_with_key(r, cleanup_key, job_name)

    if periodic(r, job_name):
        r.lrem("periodic", 1, job_name)


def periodic(r, job_name):
    periodic = r.lrange("periodic", 0, -1)
    return job_name.encode("utf-8") in periodic


@app.task(bind=True, base=IrodsTask)
def sync_path(self, meta):

    task = meta["task"]
    path = meta["path"]
    path_q_name = meta["path_queue"]
    file_q_name = meta["file_queue"]
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)
    timeout = get_timeout(logger, meta)

    max_retries = get_max_retries(logger, meta)

    try:
        r = get_redis(config)
        if isfile(path):
            logger.info("enqueue file path", path=path)
            meta = meta.copy()
            meta["task"] = "sync_file"
            async(r, logger, sync_file, meta, file_q_name)
            logger.info("succeeded", task=task, path=path)
        else:
            logger.info("walk dir", path=path)
            meta = meta.copy()
            meta["task"] = "sync_dir"
            async(r, logger, sync_dir, meta, file_q_name)
            for n in listdir(path):
                meta = meta.copy()
                meta["path"] = join(path, n)
                async(r, logger, sync_path, meta, path_q_name)
            logger.info("succeeded_dir", task=task, path=path)
    except OSError as err:
        logger.warning("failed_OSError", err=err, task=task, path=path)
    except Exception as err:
        logger.error("failed", err=err, task=task, path=path)
        retry_countdown = get_delay(logger, meta, self.request.retries + 1)
        raise self.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)


@app.task(bind=True, base=IrodsTask)
def sync_file(self, meta):
    hdlr = meta["event_handler"]
    task = meta["task"]
    path = meta["path"]
    root = meta["root"]
    target = meta["target"]
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)

    max_retries = get_max_retries(logger, meta)

    lock = None
    try:
        logger.info("synchronizing file. path = " + path)
        r = get_redis(config)
        sync_key = path + ":" + target
        lock = redis_lock.Lock(r, "sync_file:"+sync_key)
        lock.acquire()
        t = datetime.now().timestamp()
        if not all:
            sync_time = get_with_key(r, sync_time_key, sync_key, float)
        else:
            sync_time = None
        mtime = getmtime(path)
        ctime = getctime(path)
        if sync_time is None or mtime >= sync_time:
            logger.info("synchronizing file", path=path, t0=sync_time, t=t, mtime=mtime)
            meta2 = meta.copy()
            meta2["target"] = join(target, relpath(path, start=root))
            sync_irods.sync_data_from_file(meta2, logger, True)
            set_with_key(r, sync_time_key, sync_key, str(t))
            logger.info("succeeded", task=task, path=path)
        elif ctime >= sync_time:
            logger.info("synchronizing file", path=path, t0=sync_time, t=t, ctime=ctime)
            meta2 = meta.copy()
            meta2["target"] = join(target, relpath(path, start=root))
            sync_irods.sync_metadata_from_file(meta2, logger)
            set_with_key(r, sync_time_key, sync_key, str(t))
            logger.info("succeeded_metadata_only", task=task, path=path)
        else:
            logger.info("succeeded_file_has_not_changed", task=task, path=path)
    except OSError as err:
        logger.warning("failed_OSError", err=err, task=task, path=path)
    except Exception as err:
        logger.error("failed", err=err, task=task, path=path)
        retry_countdown = get_delay(logger, meta, self.request.retries + 1)
        raise self.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)
    finally:
        if lock is not None:
            lock.release()



@app.task(bind=True, base=IrodsTask)
def sync_dir(self, meta):
    hdlr = meta["event_handler"]
    task = meta["task"]
    path = meta["path"]
    root = meta["root"]
    target = meta["target"]
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)

    max_retries = get_max_retries(logger, meta)

    lock = None
    try:
        logger.info("synchronizing dir. path = " + path)
        r = get_redis(config)
        sync_key = path + ":" + target
        lock = redis_lock.Lock(r, "sync_dir:"+path)
        lock.acquire()
        t = datetime.now().timestamp()
        if not all:
            sync_time = get_with_key(r, sync_time_key, sync_key, float)
        else:
            sync_time = None
        mtime = getmtime(path)
        ctime = getctime(path)
        if sync_time is None or mtime >= sync_time:
            logger.info("synchronizing dir", path=path, t0=sync_time, t=t, mtime=mtime)
            if path == root:
                target2 = target
            else:
                target2 = join(target, relpath(path, start=root))
            meta2 = meta.copy()
            meta2["target"] = target2
            sync_irods.sync_data_from_dir(meta2, logger, True)
            set_with_key(r, sync_time_key, sync_key, str(t))
            logger.info("succeeded", task=task, path=path)
        elif ctime >= sync_time:
            logger.info("synchronizing dir", path=path, t0=sync_time, t=t, ctime=ctime)
            if path == root:
                target2 = target
            else:
                target2 = join(target, relpath(path, start=root))
            meta2 = meta.copy()
            meta2["target"] = target2
            sync_irods.sync_metadata_from_dir(meta2, logger)
            set_with_key(r, sync_time_key, sync_key, str(t))
            logger.info("succeeded_metadata_only", task=task, path=path)
        else:
            logger.info("succeeded_file_has_not_changed", task=task, path=path)
    except OSError as err:
        logger.warning("failed_OSError", err=err, task=task, path=path)
    except Exception as err:
        logger.error("failed", err=err, task=task, path=path)
        retry_countdown = get_delay(logger, meta, self.request.retries + 1)
        raise self.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)
    finally:
        if lock is not None:
            lock.release()



@app.task
def restart(meta):
    job_name = meta["job_name"]
    path_q_name = meta["path_queue"]
    restart_queue = meta["restart_queue"]
    interval = meta["interval"]
    config = meta["config"]
    logging_config = config["log"]
    if interval is not None:
        restart.s(meta).apply_async(task_id=job_name, queue=restart_queue, countdown=interval)

    logger = sync_logging.get_sync_logger(logging_config)

    timeout = get_timeout(logger, meta)

    try:
        logger.info("***************** restart *****************")
        r = get_redis(config)

        if interval is not None:
            if done(r, job_name):
                logger.info("queue empty and worker not busy")

                meta = meta.copy()
                meta["task"] = "sync_path"
                async(r, logger, sync_path, meta, path_q_name)
            else:
                logger.info("queue not empty or worker busy")
        else:
            meta = meta.copy()
            meta["task"] = "sync_path"
            async(r, logger, sync_path, meta, path_q_name)

    except OSError as err:
        logger.warning("Warning: " + str(err))
    except Exception as err:
        logger.error("Unexpected error: " + str(err))
        raise


def start_synchronization(data):

    config = data["config"]
    logging_config = config["log"]
    root = data["root"]
    job_name = data["job_name"]
    event_handler = data.get("event_handler")
    event_handler_data = data.get("event_handler_data")
    event_handler_path = data.get("event_handler_path")
    interval = data["interval"]
    restart_queue = data["restart_queue"]
    timeout = data["timeout"]

    logger = sync_logging.get_sync_logger(logging_config)

    data_copy = data.copy()
    root_abs = realpath(root)
    data_copy["root"] = root_abs
    data_copy["path"] = root_abs

    r = get_redis(config)
    with redis_lock.Lock(r, "lock:periodic"):
        if interval is not None and periodic(r, job_name):
            logger.error("job exists")
            raise Exception("job exists")

        if event_handler is None and event_handler_path is not None and event_handler_data is not None:
            event_handler = "event_handler" + uuid1().hex
            hdlr2 = event_handler_path + "/" + event_handler + ".py"
            with open(hdlr2, "w") as f:
                f.write(event_handler_data)
            set_with_key(r, cleanup_key, job_name, hdlr2.encode("utf-8"))

        if interval is not None:
            r.rpush("periodic", job_name.encode("utf-8"))
            restart.s(data_copy).apply_async(queue=restart_queue, task_id=job_name)
        else:
            restart.s(data_copy).apply_async(queue=restart_queue)


def stop_synchronization(job_name, config):
    logger = sync_logging.get_sync_logger(config["log"])

    r = get_redis(config)

    with redis_lock.Lock(r, "lock:periodic"):
        if not periodic(r, job_name):
            logger.error("job not exists")
            raise Exception("job not exists")

        app.control.revoke(job_name)

        cleanup(r, job_name)


def list_synchronization(config):
    r = get_redis(config)
    with redis_lock.Lock(r, "lock:periodic"):
        return map(lambda job_id: job_id.decode("utf-8"), r.lrange("periodic", 0, -1))
