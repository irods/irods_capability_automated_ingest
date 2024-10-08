from . import sync_logging
from .irods import irods_utils
from .redis_key import redis_key_handle
from .redis_utils import get_redis
from .sync_job import get_stopped_jobs_list, sync_job
from .tasks import filesystem_tasks, s3_bucket_tasks

from os.path import realpath
from uuid import uuid1
import json
import progressbar
import redis_lock
import textwrap
import time
import uuid

uuid_ = uuid.uuid4().hex


def stop_job(job_name, config):
    logger = sync_logging.get_sync_logger(config["log"])
    r = get_redis(config)
    with redis_lock.Lock(r, "lock:periodic"):
        job = sync_job(job_name, r)
        if job.cleanup_handle().get_value() is None:
            logger.error("job [{0}] does not exist".format(job_name))
            raise Exception("job [{0}] does not exist".format(job_name))
        job.stop()


def list_jobs(config):
    r = get_redis(config)
    with redis_lock.Lock(r, "lock:periodic"):
        periodic_jobs = list(
            map(lambda job_id: job_id.decode("utf-8"), r.lrange("periodic", 0, -1))
        )
        singlepass_jobs = list(
            map(lambda job_id: job_id.decode("utf-8"), r.lrange("singlepass", 0, -1))
        )
        jobs_map = {
            "periodic": [sync_job(job_name, r).asdict() for job_name in periodic_jobs],
            "singlepass": [
                sync_job(job_name, r).asdict() for job_name in singlepass_jobs
            ],
            "stopped": get_stopped_jobs_list(r),
        }
        return jobs_map


def monitor_job(job_name, progress, config):
    logger = sync_logging.get_sync_logger(config["log"])
    job = sync_job(job_name, get_redis(config))
    if job.cleanup_handle().get_value() is None:
        logger.error("job [{0}] does not exist".format(job.name()))
        raise Exception("job [{0}] does not exist".format(job.name()))
    try:
        if not progress:
            while not job.done() or job.periodic():
                time.sleep(1)
            if job.stopped():
                logger.warning(
                    f"Job [{job.name()}] was stopped and may not have finished."
                )
            failures = job.failures_handle().get_value()
            if failures is not None and failures != 0:
                return -1
            return 0
        start_time = job.start_time_handle().get_value()
        if start_time is None:
            logger.error(
                f"Job [{job.name()}] has no start time. Cannot display progress."
            )
            return -1
        widgets = [
            " [",
            progressbar.Variable("timer"),
            "] ",
            progressbar.Bar(),
            " (",
            progressbar.ETA(),
            ") ",
            progressbar.Variable("total"),
            " ",
            progressbar.Variable("remaining"),
            " ",
            progressbar.Variable("failed"),
            " ",
            progressbar.Variable("retried"),
        ]
        with progressbar.ProgressBar(
            max_value=1, widgets=widgets, redirect_stdout=True, redirect_stderr=True
        ) as bar:

            def update_pbar():
                job_info = job.asdict()
                total_tasks = job_info["total_tasks"]
                remaining_tasks = job_info["remaining_tasks"]
                if total_tasks == 0:
                    percentage = 0
                else:
                    percentage = max(
                        0, min(1, (total_tasks - remaining_tasks) / total_tasks)
                    )
                bar.update(
                    percentage,
                    timer=job_info["elapsed_time"],
                    total=total_tasks,
                    remaining=remaining_tasks,
                    failed=job_info["failed_tasks"],
                    retried=job_info["retried_tasks"],
                )

            while not job.done() or job.periodic():
                update_pbar()
                time.sleep(1)
            if job.stopped():
                logger.warning(
                    f"Job [{job.name()}] was stopped and may not have finished."
                )
            else:
                update_pbar()
        failures = job.failures_handle().get_value()
        if failures is not None and failures != 0:
            return -1
        else:
            return 0
    except KeyboardInterrupt:
        logger.info(f"KeyboardInterrupt stopped monitoring of job [{job.name()}].")
        return 0


def start_job(data):
    config = data["config"]
    logging_config = config["log"]
    src_path = data["src_path"]
    job_name = data["job_name"]
    interval = data["interval"]
    restart_queue = data["restart_queue"]
    sychronous = data["synchronous"]
    progress = data["progress"]
    s3_region_name = data["s3_region_name"]
    s3_endpoint_domain = data["s3_endpoint_domain"]
    s3_keypair = data["s3_keypair"]
    s3_multipart_chunksize = data["s3_multipart_chunksize_in_mib"]
    logger = sync_logging.get_sync_logger(logging_config)
    data_copy = data.copy()

    if s3_keypair is not None:
        with open(s3_keypair) as f:
            data_copy["s3_access_key"] = f.readline().rstrip()
            data_copy["s3_secret_key"] = f.readline().rstrip()
        # set source
        src_abs = src_path
        main_task = s3_bucket_tasks.s3_bucket_main_task
    else:
        src_abs = realpath(src_path)
        main_task = filesystem_tasks.filesystem_main_task

    data_copy["root"] = src_abs
    data_copy["path"] = src_abs

    irods_utils.validate_target_collection(data_copy, logger)

    def store_event_handler(data, job):
        event_handler = data.get("event_handler")
        event_handler_data = data.get("event_handler_data")
        event_handler_path = data.get("event_handler_path")

        # investigate -- kubernetes
        if (
            event_handler is None
            and event_handler_path is not None
            and event_handler_data is not None
        ):
            event_handler = "event_handler" + uuid1().hex
            hdlr2 = event_handler_path + "/" + event_handler + ".py"
            with open(hdlr2, "w") as f:
                f.write(event_handler_data)
            cleanup_list = [hdlr2.encode("utf-8")]
            data["event_handler"] = event_handler
        # if no argument is given, use default event_handler
        elif event_handler is None:
            # constructing redis_key and putting default event_handler into redis
            uuid_ = uuid.uuid4().hex
            event_handler_key = redis_key_handle(
                r, "custom_event_handler", job.name() + "::" + uuid_
            )
            content_string = textwrap.dedent(
                """
            from irods_capability_automated_ingest.core import Core 
            from irods_capability_automated_ingest.utils import Operation, DeleteMode
            class event_handler(Core):
                @staticmethod
                def operation(session, meta, *args, **options):
                    return Operation.REGISTER_SYNC

                @staticmethod
                def delete_mode(meta):
                    return DeleteMode.DO_NOT_DELETE"""
            )
            event_handler_key.set_value(content_string)

            # putting redis_key into meta map
            data_copy["event_handler_key"] = event_handler_key.get_key()

            cleanup_list = []
        else:
            # constructing redis_key and putting custom_event_handler into redis
            with open(event_handler, "r") as f:
                content_string = f.read()

            uuid_ = uuid.uuid4().hex
            event_handler_key = redis_key_handle(
                r, "custom_event_handler", job.name() + "::" + uuid_
            )
            event_handler_key.set_value(content_string)

            # putting redis_key into meta map
            data_copy["event_handler_key"] = event_handler_key.get_key()

            cleanup_list = []
        job.cleanup_handle().set_value(json.dumps(cleanup_list))

    r = get_redis(config)
    job = sync_job.from_meta(data_copy)
    with redis_lock.Lock(r, "lock:periodic"):
        if job.cleanup_handle().get_value() is not None:
            logger.error("job {0} already exists".format(job_name))
            raise Exception("job {0} already exists".format(job_name))

        store_event_handler(data_copy, job)

    if interval is not None:
        r.rpush("periodic", job_name.encode("utf-8"))

        main_task.s(data_copy).apply_async(queue=restart_queue, task_id=job_name)
    else:
        r.rpush("singlepass", job_name.encode("utf-8"))
        if not sychronous:
            main_task.s(data_copy).apply_async(queue=restart_queue)
        else:
            res = main_task.s(data_copy).apply()
            if res.failed():
                print(res.traceback)
                job.cleanup()
                return -1
            else:
                return monitor_job(job_name, progress, config)
