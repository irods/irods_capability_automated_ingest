from os.path import dirname, basename
from irods.models import Collection, DataObject
from redis import StrictRedis, ConnectionPool
from celery import Celery
import importlib

app = Celery("icai")


def size(session, path, replica_num=None, resc_name=None):
    args = [Collection.name == dirname(path), DataObject.name == basename(path)]

    if replica_num is not None:
        args.append(DataObject.replica_number == replica_num)

    if resc_name is not None:
        args.append(DataObject.resource_name == resc_name)

    for row in session.query(DataObject.size).filter(*args):
        return int(row[DataObject.size])


redis_connection_pool_map = {}


def get_redis(config):
    redis_config = config["redis"]
    host = redis_config["host"]
    port = redis_config["port"]
    db = redis_config["db"]
    url = "redis://" + host + ":" + str(port) + "/" + str(db)
    pool = redis_connection_pool_map.get(url)
    if pool is None:
        pool = ConnectionPool(host=host, port=port, db=db)
        redis_connection_pool_map[url] = pool

    return StrictRedis(connection_pool=pool)


def sync_time_key(path):
    return "sync_time:/"+path


def type_key(path):
    return "type:/"+path


def cleanup_key(job_id):
    return "cleanup:/"+job_id


def stop_key(job_id):
    return "stop:/"+job_id


def tasks_key(job_name):
    return "tasks:/"+job_name


def count_key(job_name):
    return "count:/"+job_name


def dequeue_key(job_name):
    return "dequeue:/"+job_name


def failures_key(job_name):
    return "failures:/"+job_name


def retries_key(job_name):
    return "retries:/"+job_name


def get_with_key(r, key, path, typefunc):
    sync_time_bs = r.get(key(path))
    if sync_time_bs is None:
        sync_time = None
    else:
        sync_time = typefunc(sync_time_bs)
    return sync_time


def set_with_key(r, key, path, sync_time):
    r.set(key(path), sync_time)


def reset_with_key(r, key, path):
    r.delete(key(path))


def incr_with_key(r, key, path):
    r.incr(key(path))


def decr_with_key(r, key, path):
    return r.decr(key(path))


def get_hdlr_mod(meta):
    hdlr = meta.get("event_handler")
    if hdlr is not None:
        hdlr_mod0 = importlib.import_module(hdlr)
        hdlr_mod = getattr(hdlr_mod0, "event_handler", None)
    else:
        hdlr_mod = None
    return hdlr_mod


def get_max_retries(logger, meta):
    hdlr_mod = get_hdlr_mod(meta)

    if hasattr(hdlr_mod, "max_retries"):
        max_retries = hdlr_mod.max_retries(hdlr_mod, logger, meta)
    else:
        max_retries = 0

    return max_retries


def get_timeout(logger, meta):
    hdlr_mod = get_hdlr_mod(meta)

    if hasattr(hdlr_mod, "timeout"):
        timeout = hdlr_mod.timeout(hdlr_mod, logger, meta)
    else:
        timeout = 3600

    return timeout


def get_delay(logger, meta, retries):
    hdlr_mod = get_hdlr_mod(meta)

    if hasattr(hdlr_mod, "delay"):
        delay = hdlr_mod.delay(hdlr_mod, logger, meta, retries)
    else:
        delay = 0

    return delay


def call(hdlr_mod, hdlr, func, logger, *args, **options):
    if hasattr(hdlr_mod, hdlr):
        logger.debug("calling " + hdlr + " event handler: args = " + str(args) + ", options = " + str(options))
        getattr(hdlr_mod, hdlr)(func, *args, **options)
    else:
        func(*args, **options)

