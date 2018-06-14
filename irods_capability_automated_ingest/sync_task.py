from rq import Queue, Worker, get_current_job
from rq.worker import WorkerStatus
from os import listdir
import os
from os.path import isfile, join, getmtime, realpath, relpath, getctime
from datetime import datetime
from rq_scheduler import Scheduler
import redis_lock
from irods_capability_automated_ingest import sync_logging, sync_irods
from irods_capability_automated_ingest.sync_utils import get_redis
import time
from uuid import uuid1

def sync_time_key(path):
    return "sync_time:/"+path

def type_key(path):
    return "type:/"+path

def cleanup_key(job_id):
    return "cleanup:/"+job_id

def get_with_key(r, key, path, typefunc):
    sync_time_bs = r.get(key(path))
    if sync_time_bs == None:
        sync_time = None
    else:
        sync_time = typefunc(sync_time_bs)
    return sync_time

def set_with_key(r, key, path, sync_time):
    r.set(key(path), sync_time)

def reset_with_key(r, key, path):
    r.delete(key(path))

def sync_path():

    job = get_current_job()
    task = job.meta["task"]
    path = job.meta["path"]
    path_q_name = job.meta["path_queue"]
    file_q_name = job.meta["file_queue"]
    config = job.meta["config"]
    logging_config = config["log"]
    depends_on = job.meta.get("depends_on")
    timeout = job.meta["timeout"]

    logger = sync_logging.create_sync_logger(logging_config)

    try:
        r = get_redis(config)
        if isfile(path):
            logger.info("enqueue file path", path = path)
            q = Queue(file_q_name, connection=r)
            meta = job.meta.copy()
            del meta["depends_on"]
            meta["task"] = "sync_file"
            q.enqueue(sync_file, meta=meta, depends_on=depends_on, timeout=timeout)
            logger.info("succeeded", task=task, path = path)
        else:
            logger.info("walk dir", path = path)
            q = Queue(path_q_name, connection=r)
            meta = job.meta.copy()
            del meta["depends_on"]
            meta["task"] = "sync_dir"
            job_id = q.enqueue(sync_dir, meta=meta, depends_on=depends_on, timeout=timeout).id
            for n in listdir(path):
                meta = job.meta.copy()
                meta["depends_on"] = job_id
                meta["path"] = join(path, n)
                q.enqueue(sync_path, meta=meta, timeout=timeout)
            logger.info("succeeded_dir", task=task, path = path)
    except OSError as err:
        logger.warning("failed_OSError", err=err, task=task, path = path)
    except Exception as err:
        logger.error("failed", err=err, task=task, path = path)
        raise

def sync_file():
    job = get_current_job()
    hdlr = job.meta["event_handler"]
    task = job.meta["task"]
    path = job.meta["path"]
    root = job.meta["root"]
    target = job.meta["target"]
    config = job.meta["config"]
    logging_config = config["log"]
    all = job.meta["all"]

    logger = sync_logging.create_sync_logger(logging_config)
    try:
        logger.info("synchronizing file. path = " + path)
        r = get_redis(config)
        with redis_lock.Lock(r, path):
            t = datetime.now().timestamp()
            if not all:
                sync_time = get_with_key(r, sync_time_key, path, float)
            else:
                sync_time = None
            mtime = getmtime(path)
            ctime = getctime(path)
            if sync_time == None or mtime >= sync_time:
                logger.info("synchronizing file", path = path, t0 = sync_time, t = t, mtime = mtime)
                sync_irods.sync_data_from_file(join(target, relpath(path, start=root)), path, hdlr, logger, True)
                set_with_key(r, sync_time_key, path, str(t))
                logger.info("succeeded", task=task, path = path)
            elif ctime >= sync_time:
                logger.info("synchronizing file", path = path, t0 = sync_time, t = t, ctime = ctime)
                sync_irods.sync_metadata_from_file(join(target, relpath(path, start=root)), path, hdlr, logger)
                set_with_key(r, sync_time_key, path, str(t))
                logger.info("succeeded_metadata_only", task=task, path = path)
            else:
                logger.info("succeeded_file_has_not_changed", task=task, path = path)
    except OSError as err:
        logger.warning("failed_OSError", err=err, task=task, path = path)
    except Exception as err:
        logger.error("failed", err=err, task=task, path = path)
        raise


def sync_dir():
    job = get_current_job()
    hdlr = job.meta["event_handler"]
    task = job.meta["task"]
    path = job.meta["path"]
    root = job.meta["root"]
    target = job.meta["target"]
    config = job.meta["config"]
    logging_config = config["log"]
    all = job.meta["all"]

    logger = sync_logging.create_sync_logger(logging_config)
    try:
        logger.info("synchronizing dir. path = " + path)
        r = get_redis(config)
        with redis_lock.Lock(r, path):
            t = datetime.now().timestamp()
            if not all:
                sync_time = get_with_key(r, sync_time_key, path, float)
            else:
                sync_time = None
            mtime = getmtime(path)
            ctime = getctime(path)
            if sync_time == None or mtime >= sync_time:
                logger.info("synchronizing dir", path = path, t0 = sync_time, t = t, mtime = mtime)
                if path == root:
                    target2 = target
                else:
                    target2 = join(target, relpath(path, start=root))
                sync_irods.sync_data_from_dir(target2, path, hdlr, logger, True)
                set_with_key(r, sync_time_key, path, str(t))
                logger.info("succeeded", task=task, path = path)
            elif ctime >= sync_time:
                logger.info("synchronizing dir", path = path, t0 = sync_time, t = t, ctime = ctime)
                sync_irods.sync_metadata_from_dir(join(target, relpath(path, start=root)), path, hdlr, logger)
                set_with_key(r, sync_time_key, path, str(t))
                logger.info("succeeded_metadata_only", task=task, path = path)
            else:
                logger.info("succeeded_file_has_not_changed", task=task, path = path)
    except OSError as err:
        logger.warning("failed_OSError", err=err, task=task, path = path)
    except Exception as err:
        logger.error("failed", err=err, task=task, path = path)
        raise


def cleanup(r, job_name):
    hdlr = get_with_key(r, cleanup_key, job_name, lambda x: x)
    if hdlr is not None:
        os.remove(hdlr.decode("utf-8"))
        reset_with_key(r, cleanup_key, job_name)

    if periodic(r, job_name):
        r.lrem("periodic", 1, job_name)
        

def periodic(r, job_name):
    periodic = r.lrange("periodic", 0, -1)
    return job_name not in periodic
        
def restart():
    job = get_current_job()
    job_name = job.meta["job_name"]
    path_q_name = job.meta["path_queue"]
    file_q_name = job.meta["file_queue"]
    config = job.meta["config"]
    logging_config = config["log"]
    timeout = job.meta["timeout"]

    logger = sync_logging.create_sync_logger(logging_config)
    try:
        logger.info("***************** restart *****************")
        r = get_redis(config)
        path_q = Queue(path_q_name, connection=r)
        file_q = Queue(file_q_name, connection=r)
        path_q_workers = Worker.all(queue=path_q)
        file_q_workers = Worker.all(queue=file_q)

        job_id = get_current_job().id
        def all_not_busy(ws):
            return all(w.get_state() != WorkerStatus.BUSY or w.get_current_job_id() == job_id for w in ws)

        # this doesn't guarantee that there is only one tree walk, but it prevents tree walk when the file queue is not empty
        if periodic(r, job_name) and path_q.is_empty() and file_q.is_empty() and all_not_busy(path_q_workers) and all_not_busy(file_q_workers):
            logger.info("queue empty and worker not busy")

            meta = job.meta.copy()
            meta["depends_on"] = None
            meta["task"] = "sync_path"
            path_q.enqueue(sync_path, meta=meta, timeout=timeout)
        else:
            logger.info("queue not empty or worker busy")

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

    logger = sync_logging.create_sync_logger(logging_config)

    data_copy = data.copy()
    root_abs = realpath(root)
    data_copy["root"] = root_abs
    data_copy["path"] = root_abs

    r = get_redis(config)
    scheduler = Scheduler(connection=r)

    if job_name.encode("utf-8") in r.lrange("periodic", 0, -1):
        logger.error("job exists")
        raise Exception("job exists")

    if event_handler is None and event_handler_path is not None and event_handler_data is not None:
        event_handler = "event_handler" + uuid1().hex
        hdlr2 = event_handler_path + "/" + event_handler + ".py"
        with open(hdlr2, "w") as f:
            f.write(event_handler_data)
        set_with_key(r, cleanup_key, job_name, hdlr2.encode("utf-8"))

    if interval is not None:
        scheduler.schedule(
            scheduled_time=datetime.utcnow(),
            func=restart,
            args=[],
            interval=interval,
            queue_name=restart_queue,
            id=job_name,
            meta=data_copy,
            timeout=timeout
        )
        r.rpush("periodic", job_name.encode("utf-8"))
    else:
        restart_q = Queue(restart_queue, connection=r)
        restart_q.enqueue(restart, job_id=job_name, meta=data_copy, timeout=timeout)


def stop_synchronization(job_name, config):
    logger = sync_logging.create_sync_logger(config["log"])

    r = get_redis(config)
    scheduler = Scheduler(connection=r)

    if job_name.encode("utf-8") not in r.lrange("periodic", 0, -1):
        logger.error("job not exists")
        raise Exception("job not exists")

    while scheduler.cancel(job_name) == 0:
        time.sleep(.1)

    cleanup(r, job_name)
    
def list_synchronization(config):
    logger = sync_logging.create_sync_logger(config["log"])
    r = get_redis(config)
    # scheduler = Scheduler(connection=r)
    # list_of_job_instances = scheduler.get_jobs()
    # for job_instance in list_of_job_instances:
    #     job_id = job_instance.id
    #     print(job_id)
    return map(lambda job_id : job_id.decode("utf-8"), r.lrange("periodic",0,-1))


