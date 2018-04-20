from rq import Queue, Worker, get_current_job
from rq.worker import WorkerStatus
from os import listdir
from os.path import isfile, join, getmtime, realpath, relpath, getctime
from datetime import datetime
from rq_scheduler import Scheduler
import redis_lock
from irods_capability_automated_ingest import sync_logging, sync_irods
from irods_capability_automated_ingest.sync_utils import get_redis
import time


def sync_time_key(path):
    return "sync_time:/"+path

def type_key(path):
    return "type:/"+path

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

def sync_path(path_q_name, file_q_name, target, root, path, hdlr, logging_config):
    logger = sync_logging.create_sync_logger(logging_config)
    try:
        r = get_redis(logging_config)
        if isfile(path):
            logger.info("enqueue file path " + path)
            q = Queue(file_q_name, connection=r)
            q.enqueue(sync_file, target, root, path, hdlr, logging_config)
        else:
            logger.info("walk dir " + path)
            q = Queue(path_q_name, connection=r)
            for n in listdir(path):
                q.enqueue(sync_path, path_q_name, file_q_name, target, root, join(path, n), hdlr, logging_config)
    except OSError as err:
        logger.warning("Warning: " + str(err))        
    except Exception as err:
        logger.error("Unexpected error: " + str(err))
        raise

def sync_file(target, root, path, hdlr, logging_config):
    logger = sync_logging.create_sync_logger(logging_config)
    try:
        logger.info("synchronizing file. path = " + path)
        r = get_redis(logging_config)
        with redis_lock.Lock(r, path):
            t = datetime.now().timestamp()
            sync_time = get_with_key(r, sync_time_key, path, float)
            mtime = getmtime(path)
            ctime = getctime(path)
            if sync_time == None or mtime >= sync_time:
                logger.info("synchronizing file. path = " + path + ", t0 = " + str(sync_time) + ", t = " + str(t) + ", mtime = " + str(mtime) + ".")
                sync_irods.sync_data_from_file(join(target, relpath(path, start=root)), path, hdlr, True)
                set_with_key(r, sync_time_key, path, str(t))
            elif ctime >= sync_time:
                logger.info("synchronizing file. path = " + path + ", t0 = " + str(sync_time) + ", t = " + str(t) + ", ctime = " + str(ctime) + ".")
                sync_irods.sync_metadata_from_file(join(target, relpath(path, start=root)), path, hdlr)
                set_with_key(r, sync_time_key, path, str(t))
            else:
                logger.info("file hasn't changed. path = " + path + ".")
    except OSError as err:
        logger.warning("Warning: " + str(err))        
    except Exception as err:
        logger.error("Unexpected error: " + str(err))
        raise

def restart(path_q_name, file_q_name, target, root, path, hdlr, logging_config):
    logger = sync_logging.create_sync_logger(logging_config)
    try:
        logger.info("***************** restart *****************")
        r = get_redis(logging_config)
        path_q = Queue(path_q_name, connection=r)
        file_q = Queue(file_q_name, connection=r)
        path_q_workers = Worker.all(queue=path_q)
        file_q_workers = Worker.all(queue=file_q)

        job_id = get_current_job().id
        def all_not_busy(ws):
            return all(w.get_state() != WorkerStatus.BUSY or w.get_current_job_id() == job_id for w in ws)            

        # this doesn't guarantee that there is only one tree walk, but it prevents tree walk when the file queue is not empty
        if path_q.is_empty() and file_q.is_empty() and all_not_busy(path_q_workers) and all_not_busy(file_q_workers):
            logger.info("queue empty and worker not busy")
            path_q.enqueue(sync_path, path_q_name, file_q_name, target, root, path, hdlr, logging_config)
        else:
            logger.info("queue not empty or worker busy")

    except OSError as err:
        logger.warning("Warning: " + str(err))        
    except Exception as err:
        logger.error("Unexpected error: " + str(err))
        raise

def start_synchronization(restart_q_name, path_q_name, file_q_name, target, root, interval, job_name, hdlr, logging_config):
    logger = sync_logging.create_sync_logger(logging_config)

    root_abs = realpath(root)

    r = get_redis(logging_config)
    scheduler = Scheduler(connection=r)
    
    if job_name in scheduler:
        logger.error("job exists")
        return

    if interval is not None:
        scheduler.schedule(
            scheduled_time = datetime.utcnow(),
            func = restart,
            args = [path_q_name, file_q_name, target, root_abs, root_abs, hdlr, logging_config],
            interval = interval,
            queue_name = restart_q_name,
            id = job_name
        )
        r.rpush("periodic", job_name.encode("utf-8"))
    else:
        restart_q = Queue(restart_q_name, connection=r)
        restart_q.enqueue(restart, path_q_name, file_q_name, target, root_abs, root_abs, hdlr, logging_config, job_id=job_name)
        
def stop_synchronization(job_name, logging_config):
    logger = sync_logging.create_sync_logger(logging_config)

    r = get_redis(logging_config)
    scheduler = Scheduler(connection=r)
    
    if job_name.encode("utf-8") not in r.lrange("periodic", 0, -1):
        logger.error("job not exists", job_name.encode("utf-8"), r.lrange("periodic",0,-1))
        return

    while scheduler.cancel(job_name) == 0:
        time.sleep(.1)
        
    r.lrem("periodic", 1, job_name)

def list_synchronization(logging_config):
    logger = sync_logging.create_sync_logger(logging_config)
    r = get_redis(logging_config)
    # scheduler = Scheduler(connection=r)
    # list_of_job_instances = scheduler.get_jobs()
    # for job_instance in list_of_job_instances:
    #     job_id = job_instance.id
    #     print(job_id)
    for job_id in r.lrange("periodic",0,-1):
        print(job_id.decode("utf-8"))
    
    
