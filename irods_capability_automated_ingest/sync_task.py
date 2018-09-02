# Use the built-in version of scandir/walk if possible, otherwise
# use the scandir module version
try:
    from os import scandir
except ImportError:
    from scandir import scandir

import stat
from os import listdir
import os
from os.path import isfile, join, getmtime, realpath, relpath, getctime, isdir, islink
from datetime import datetime
import redis_lock
from . import sync_logging, sync_irods
from .sync_utils import get_redis, app, get_with_key, get_max_retries, tasks_key, set_with_key, decr_with_key, \
    incr_with_key, reset_with_key, cleanup_key, sync_time_key, get_timeout, failures_key, retries_key, get_delay, \
    count_key, stop_key, dequeue_key, get_hdlr_mod
from .utils import retry
from uuid import uuid1
import time
import progressbar
import json
import traceback
import base64
import socket
from celery.signals import before_task_publish, after_task_publish, task_prerun, task_postrun, task_retry, task_success, task_failure, task_revoked, task_unknown, task_rejected
from billiard import current_process

import re

@task_prerun.connect()
def task_prerun(task_id=None, task=None, args=None, kwargs=None, **kw):
    meta = args[0]
    if meta["profile"]:
        config = meta["config"]
        profile_log = config.get("profile")
        logger = sync_logging.get_sync_logger(profile_log)
        logger.info("task_prerun", event_id=task_id, event_name=task.name, path=meta.get("path"), target=meta.get("target"), hostname=task.request.hostname, index=current_process().index)


@task_postrun.connect()
def task_postrun(task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, **kw):
    meta = args[0]
    if meta["profile"]:
        config = meta["config"]
        profile_log = config.get("profile")
        logger = sync_logging.get_sync_logger(profile_log)
        logger.info("task_postrun", event_id=task_id, event_name=task.name, path=meta.get("path"), target=meta.get("target"), hostname=task.request.hostname, index=current_process().index,state=state)

class IrodsTask(app.Task):

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job_name = meta["job_name"]
        logger = sync_logging.get_sync_logger(config["log"])
        r = get_redis(config)
        logger.error('failed_task', task=meta["task"], path=meta["path"], job_name=job_name, task_id=task_id, exc=exc, einfo=einfo, traceback=traceback.extract_tb(exc.__traceback__))
        incr_with_key(r, failures_key, job_name)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job_name = meta["job_name"]
        logger = sync_logging.get_sync_logger(config["log"])
        r = get_redis(config)
        logger.warning('retry_task', task=meta["task"], path=meta["path"], job_name=job_name, task_id=task_id, exc=exc, einfo=einfo, traceback=traceback.extract_tb(exc.__traceback__))
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
        logger.info('decr_job_name', task=meta["task"], path=meta["path"], job_name=job_name, task_id=task_id, retval=retval)

        r = get_redis(config)
        done = retry(logger, decr_with_key, r, tasks_key, job_name) == 0 and not retry(logger, periodic, r, job_name)
        if done:
            retry(logger, cleanup, r, job_name)

        r.rpush(dequeue_key(job_name), task_id)

        if done:
            hdlr_mod = get_hdlr_mod(meta)
            if hdlr_mod is not None and hasattr(hdlr_mod, "post_job"):
                hdlr_mod.post_job(hdlr_mod, logger, meta)


class RestartTask(app.Task):

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job_name = meta["job_name"]
        logger = sync_logging.get_sync_logger(config["log"])
        logger.error('failed_restart', path=meta["path"], job_name=job_name, task_id=task_id, exc=exc, einfo=einfo, traceback=traceback.extract_tb(exc.__traceback__))


def async(r, logger, task, meta, queue):
    job_name = meta["job_name"]
    if get_with_key(r, stop_key, job_name, str) is None:
        logger.info('incr_job_name', task=meta["task"], path=meta["path"], job_name=job_name)
        incr_with_key(r, tasks_key, job_name)
        task_id = str(uuid1())
        timeout = get_timeout(logger, meta)
        r.rpush(count_key(job_name), task_id)
        task.s(meta).apply_async(queue=queue, task_id=task_id, soft_time_limit=timeout)
    else:
        logger.info('async_job_name_stopping', task=meta["task"], path=meta["path"], job_name=job_name)


def done(r, job_name):
    ntasks = get_with_key(r, tasks_key, job_name, int)
    return ntasks is None or ntasks == 0


'''
resource | scope | reset
count_key | restart | init
dequeue_key | restart | init
tasks_key | restart | init
failures_key | restart | init
retries_key | restart | init
cleanup_key | job | cleanup
event_handlers | job | cleanup
singlepass | job | cleanup
periodic | job | cleanup
'''


def init(r, job_name):
    reset_with_key(r, count_key, job_name)
    reset_with_key(r, dequeue_key, job_name)
    reset_with_key(r, tasks_key, job_name)
    reset_with_key(r, failures_key, job_name)
    reset_with_key(r, retries_key, job_name)


def interrupt(r, job_name, cli=True, terminate=True):
    set_with_key(r, stop_key, job_name, "")
    tasks = list(map(lambda x: x.decode("utf-8"), r.lrange(count_key(job_name), 0, -1)))
    tasks2 = set(map(lambda x: x.decode("utf-8"), r.lrange(dequeue_key(job_name), 0, -1)))

    tasks = [item for item in tasks if item not in tasks2]

    if cli:
        tasks = progressbar.progressbar(tasks, max_value=len(tasks))

    for task in tasks:
        app.control.revoke(task, terminate=terminate)

    # TODO stop restart job

    reset_with_key(r, stop_key, job_name)


def cleanup(r, job_name, ):
    hdlr = get_with_key(r, cleanup_key, job_name, lambda bs: json.loads(bs.decode("utf-8")))
    for f in hdlr:
        os.remove(f)

    if periodic(r, job_name):
        r.lrem("periodic", 1, job_name)
    else:
        r.lrem("singlepass", 1, job_name)

    reset_with_key(r, cleanup_key, job_name)


def periodic(r, job_name):
    periodic = r.lrange("periodic", 0, -1)
    return job_name.encode("utf-8") in periodic

def exclude_file_type(ex_list, dir_regex, file_regex, full_path, logger):
    if len(ex_list) <= 0 and None == dir_regex and None == file_regex:
        return False

    ret_val = False
    mode    = None

    dir_match = None
    for d in dir_regex:
        dir_match = None != d.match(full_path)
        if(dir_match == True):
            break

    file_match = None
    for f in file_regex:
        file_match = None != f.match(full_path)
        if(file_match == True):
            break

    try:
        mode = os.lstat(full_path).st_mode
    except FileNotFoundError:
        return False

    if stat.S_ISREG(mode):
        if 'regular' in ex_list or file_match:
            ret_val = True
    elif stat.S_ISDIR(mode):
        if 'directory' in ex_list or dir_match:
            ret_val = True
    elif stat.S_ISCHR(mode):
        if 'character' in ex_list or file_match:
            ret_val = True
    elif stat.S_ISBLK(mode):
        if 'block' in ex_list or file_match:
            ret_val = True
    elif stat.S_ISSOCK(mode):
        if 'socket' in ex_list or file_match:
            ret_val = True
    elif stat.S_ISFIFO(mode):
        if 'pipe' in ex_list or file_match:
            ret_val = True
    elif stat.S_ISLNK(mode):
        if 'link' in ex_list or file_match:
            ret_val = True

    return ret_val


@app.task(bind=True, base=IrodsTask)
def sync_path(self, meta):

    task = meta["task"]
    path = meta["path"]
    path_q_name = meta["path_queue"]
    file_q_name = meta["file_queue"]
    config = meta["config"]
    logging_config = config["log"]
    list_dir = meta["list_dir"]
    dir_list = meta["scan_dir_list"]
    cached_is_file = meta.get("is_file")
    cached_is_dir = meta.get("is_dir")
    cached_is_link = meta.get("is_link")
    exclude_type_list = meta['exclude_file_type']
    exclude_file_name = meta['exclude_file_name']
    exclude_directory_name = meta['exclude_directory_name']

    logger = sync_logging.get_sync_logger(logging_config)

    file_regex = [ re.compile(r) for r in exclude_file_name ]
    dir_regex  = [ re.compile(r) for r in exclude_directory_name ]

    max_retries = get_max_retries(logger, meta)

    try:
        r = get_redis(config)
        if (cached_is_link is not None and cached_is_link) or islink(path):
            logger.info("enqueue link path", path=path)
            meta = meta.copy()
            meta["task"] = "sync_link"
            async(r, logger, sync_link, meta, file_q_name)
            logger.info("succeeded", task=task, path=path)
        elif (cached_is_file is not None and cached_is_file) or isfile(path):
            logger.info("enqueue file path", path=path)
            meta = meta.copy()
            meta["task"] = "sync_file"
            async(r, logger, sync_file, meta, file_q_name)
            logger.info("succeeded", task=task, path=path)
        elif (cached_is_dir is not None and cached_is_dir) or isdir(path):
            logger.info("walk dir", path=path)
            meta = meta.copy()
            meta["task"] = "sync_dir"
            async(r, logger, sync_dir, meta, file_q_name)
            if dir_list:
                if meta["profile"]:
                    config = meta["config"]
                    profile_log = config.get("profile")
                    profile_logger = sync_logging.get_sync_logger(profile_log)
                    task_id = self.request.id
                    profile_logger.info("list_dir_prerun", event_id=task_id + ":list_dir", event_name="list_dir", hostname=self.request.hostname, index=current_process().index)

            if list_dir:
                itr = listdir(path)
            else:
                itr = scandir(path)

            if dir_list:
                itr = list(itr)
                if meta["profile"]:
                    profile_logger.info("list_dir_postrun", event_id=task_id + ":list_dir", event_name="list_dir", hostname=self.request.hostname, index=current_process().index)

            for n in itr:
                meta = meta.copy()
                if list_dir:
                    full_path = os.path.join(path, n)

                    if exclude_file_type(exclude_type_list, dir_regex, file_regex, full_path, logger):
                        continue

                    logger.info('PROCESSING: full_path['+full_path+']', task=task, path=path)
                    if islink(full_path):
                        logger.info('PROCESSING: ['+full_path+'] is a LINK', task=task, path=path)

                        meta["is_file"] = False
                        meta["is_dir"] = False
                        meta["is_link"] = True
                        meta["is_socket"] = False
                        meta["path"] = full_path
                        meta["ctime"] = os.lstat(full_path).st_ctime
                        meta["mtime"] = os.lstat(full_path).st_mtime
                    else:
                        mode = os.stat(full_path).st_mode
                        if stat.S_ISSOCK(mode):
                            logger.info('PROCESSING: ['+full_path+'] is a SOCKET', task=task, path=path)

                            meta["is_file"] = False
                            meta["is_dir"] = False
                            meta["is_link"] = True
                            meta["is_socket"] = True
                            meta["path"] = full_path
                            meta["ctime"] = os.stat(full_path).st_ctime
                            meta["mtime"] = os.stat(full_path).st_mtime
                        else:
                            meta["path"] = full_path
                            meta["is_link"] = False
                else:
                    full_path = os.path.abspath(n.path)

                    if exclude_file_type(exclude_type_list, dir_regex, file_regex, full_path, logger):
                        continue

                    if islink(full_path):
                        logger.info('PROCESSING: ['+n.name+'] is a LINK', task=task, path=path)

                        meta["is_file"] = False
                        meta["is_dir"] = False
                        meta["is_link"] = True
                        meta["is_socket"] = False
                        meta["path"] = full_path
                        meta["ctime"] = os.lstat(full_path).st_ctime
                        meta["mtime"] = os.lstat(full_path).st_mtime
                    else:
                        mode = os.stat(full_path).st_mode
                        if stat.S_ISSOCK(mode):
                            meta["is_file"] = False
                            meta["is_dir"] = False
                            meta["is_link"] = True
                            meta["is_socket"] = True
                            meta["path"] = full_path
                            meta["ctime"] = os.stat(full_path).st_ctime
                            meta["mtime"] = os.stat(full_path).st_mtime
                        else:
                            meta["path"] = full_path
                            meta["is_file"] = n.is_file() or not n.is_dir()
                            meta["is_dir"] = n.is_dir()
                            meta["is_link"] = False
                            meta["is_socket"] = False
                            meta["ctime"] = n.stat().st_ctime
                            meta["mtime"] = n.stat().st_mtime
                async(r, logger, sync_path, meta, path_q_name)
            logger.info("succeeded_dir", task=task, path=path)
        else:
            raise RuntimeError("path does not exist")

    except Exception as err:
        retry_countdown = get_delay(logger, meta, self.request.retries + 1)
        raise self.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)


@app.task(bind=True, base=IrodsTask)
def sync_file(self, meta):
    sync_entry(self, meta, "file", sync_irods.sync_data_from_file, sync_irods.sync_metadata_from_file)

@app.task(bind=True, base=IrodsTask)
def sync_link(self, meta):
    sync_entry(self, meta, "link", sync_irods.sync_data_from_link, sync_irods.sync_metadata_from_link)

@app.task(bind=True, base=IrodsTask)
def sync_dir(self, meta):
    sync_entry(self, meta, "dir", sync_irods.sync_data_from_dir, sync_irods.sync_metadata_from_dir)

def sync_entry(self, meta, cls, datafunc, metafunc):
    hdlr = meta["event_handler"]
    task = meta["task"]
    path = meta["path"]
    root = meta["root"]
    target = meta["target"]
    config = meta["config"]
    logging_config = config["log"]
    ignore_cache = meta["ignore_cache"]
    logger = sync_logging.get_sync_logger(logging_config)

    max_retries = get_max_retries(logger, meta)

    lock = None
    logger.info("synchronizing " + cls + ". path = " + path)
    r = get_redis(config)
    try:
        # Attempt to encode full physical path on local filesystem
        # Special handling required for non-encodable strings which raise UnicodeEncodeError
        utf8_encode_test = path.encode('utf8')
        sync_key = path + ":" + target
    except UnicodeEncodeError:
        abspath = os.path.abspath(path)
        hostname = socket.getfqdn()
        path = os.path.dirname(abspath)
        utf8_abspath = abspath.encode('utf8', 'surrogateescape')
        b64_path_str = base64.b64encode(utf8_abspath)

        file_unique_id = str(utf8_abspath)
        unicode_error_filename = 'irods_UnicodeEncodeError_' + str(base64.b64encode(file_unique_id.encode('utf8')).decode('utf8'))

        logger.warning('sync_entry raised UnicodeEncodeError while syncing path:' + str(utf8_abspath))

        meta['path'] = path
        meta['b64_path_str'] = b64_path_str
        meta['unicode_error_filename'] = unicode_error_filename

        sync_key = file_unique_id + ":" + target
    try:
        lock = redis_lock.Lock(r, "sync_" + cls + ":"+sync_key)
        lock.acquire()
        t = datetime.now().timestamp()
        if not ignore_cache:
            sync_time = get_with_key(r, sync_time_key, sync_key, float)
        else:
            sync_time = None

        mtime = meta.get("mtime")
        if mtime is None:
            mtime = getmtime(path)

        ctime = meta.get("ctime")
        if ctime is None:
            ctime = getctime(path)

        if sync_time is None or mtime >= sync_time:
            logger.info("synchronizing " + cls, path=path, t0=sync_time, t=t, mtime=mtime)
            if path == root:
                if 'unicode_error_filename' in meta:
                    target2 = join(target, meta['unicode_error_filename'])
                else:
                    target2 = target
            else:
                target2 = join(target, relpath(path, start=root))
            meta2 = meta.copy()
            meta2["target"] = target2
            datafunc(meta2, logger, True)
            set_with_key(r, sync_time_key, sync_key, str(t))
            logger.info("succeeded", task=task, path=path)
        elif ctime >= sync_time:
            logger.info("synchronizing dir", path=path, t0=sync_time, t=t, ctime=ctime)
            if path == root:
                if 'unicode_error_filename' in meta:
                    target2 = join(target, meta['unicode_error_filename'])
                else:
                    target2 = target
            else:
                target2 = join(target, relpath(path, start=root))
            meta2 = meta.copy()
            meta2["target"] = target2
            metafunc(meta2, logger)
            set_with_key(r, sync_time_key, sync_key, str(t))
            logger.info("succeeded_metadata_only", task=task, path=path)
        else:
            logger.info("succeeded_" + cls + "_has_not_changed", task=task, path=path)
    except Exception as err:
        retry_countdown = get_delay(logger, meta, self.request.retries + 1)
        raise self.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)
    finally:
        if lock is not None:
            lock.release()


@app.task(base=RestartTask)
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

    hdlr_mod = get_hdlr_mod(meta)

    try:

        if hdlr_mod is not None and hasattr(hdlr_mod, "pre_job"):
            hdlr_mod.pre_job(hdlr_mod, logger, meta)
        logger.info("***************** restart *****************")
        r = get_redis(config)

        if not periodic(r, job_name) or done(r, job_name):
            logger.info("queue empty and worker not busy")

            init(r, job_name)
            meta = meta.copy()
            meta["task"] = "sync_path"
            async(r, logger, sync_path, meta, path_q_name)
        else:
            logger.info("queue not empty or worker busy")

    except OSError as err:
        logger.warning("Warning: " + str(err), traceback=traceback.extract_tb(err.__traceback__))
    except Exception as err:
        logger.error("Unexpected error: " + str(err), traceback=traceback.extract_tb(err.__traceback__))
        raise



def start_synchronization(data):

    config = data["config"]
    logging_config = config["log"]
    root = data["root"]
    job_name = data["job_name"]
    interval = data["interval"]
    restart_queue = data["restart_queue"]
    sychronous = data["synchronous"]
    progress = data["progress"]

    logger = sync_logging.get_sync_logger(logging_config)

    data_copy = data.copy()
    root_abs = realpath(root)
    data_copy["root"] = root_abs
    data_copy["path"] = root_abs

    def store_event_handler(data):
        event_handler = data.get("event_handler")
        event_handler_data = data.get("event_handler_data")
        event_handler_path = data.get("event_handler_path")

        if event_handler is None and event_handler_path is not None and event_handler_data is not None:
            event_handler = "event_handler" + uuid1().hex
            hdlr2 = event_handler_path + "/" + event_handler + ".py"
            with open(hdlr2, "w") as f:
                f.write(event_handler_data)
            cleanup_list = [hdlr2.encode("utf-8")]
            data["event_handler"] = event_handler
        else:
            cleanup_list = []
        set_with_key(r, cleanup_key, job_name, json.dumps(cleanup_list))

    r = get_redis(config)
    with redis_lock.Lock(r, "lock:periodic"):
        if get_with_key(r, cleanup_key, job_name, str) is not None:
            logger.error("job exists")
            raise Exception("job exists")

        store_event_handler(data_copy)

    if interval is not None:
        r.rpush("periodic", job_name.encode("utf-8"))

        restart.s(data_copy).apply_async(queue=restart_queue, task_id=job_name)
    else:
        r.rpush("singlepass", job_name.encode("utf-8"))
        if not sychronous:
            restart.s(data_copy).apply_async(queue=restart_queue)
        else:
            res = restart.s(data_copy).apply()
            if res.failed():
                print(res.traceback)
                cleanup(r, job_name)
                return -1
            else:
                return monitor_synchronization(job_name, progress, config)


def monitor_synchronization(job_name, progress, config):

    logging_config = config["log"]

    logger = sync_logging.get_sync_logger(logging_config)

    r = get_redis(config)
    if get_with_key(r, cleanup_key, job_name, str) is None:
        logger.error("job [{0}] does not exist".format(job_name))
        raise Exception("job [{0}] does not exist".format(job_name))

    if progress:

        widgets = [
            ' [', progressbar.Timer(), '] ',
            progressbar.Bar(),
            ' (', progressbar.ETA(), ') ',
            progressbar.DynamicMessage("count"), " ",
            progressbar.DynamicMessage("tasks"), " ",
            progressbar.DynamicMessage("failures"), " ",
            progressbar.DynamicMessage("retries")
        ]

        with progressbar.ProgressBar(max_value=1, widgets=widgets, redirect_stdout=True, redirect_stderr=True) as bar:
            def update_pbar():
                total2 = get_with_key(r, tasks_key, job_name, int)
                total = r.llen(count_key(job_name))
                if total == 0:
                    percentage = 0
                else:
                    percentage = max(0, min(1, (total - total2) / total))

                failures = get_with_key(r, failures_key, job_name, int)
                retries = get_with_key(r, retries_key, job_name, int)

                bar.update(percentage, count=total, tasks=total2, failures=failures, retries=retries)

            while not done(r, job_name) or periodic(r, job_name):
                update_pbar()
                time.sleep(1)

            update_pbar()

    else:
        while not done(r, job_name) or periodic(r, job_name):
            time.sleep(1)

    failures = get_with_key(r, failures_key, job_name, int)
    if failures != 0:
        return -1
    else:
        return 0


def stop_synchronization(job_name, config):
    logger = sync_logging.get_sync_logger(config["log"])

    r = get_redis(config)

    with redis_lock.Lock(r, "lock:periodic"):
        if get_with_key(r, cleanup_key, job_name, str) is None:
            logger.error("job [{0}] does not exist".format(job_name))
            raise Exception("job [{0}] does not exist".format(job_name))
        else:
            interrupt(r, job_name)
            cleanup(r, job_name)


def list_synchronization(config):
    r = get_redis(config)
    with redis_lock.Lock(r, "lock:periodic"):
        return {"periodic":list(map(lambda job_id: job_id.decode("utf-8"), r.lrange("periodic", 0, -1))),
                "singlepass":list(map(lambda job_id: job_id.decode("utf-8"), r.lrange("singlepass", 0, -1)))}
