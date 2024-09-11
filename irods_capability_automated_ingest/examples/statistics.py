from .. import redis_key
from ..redis_utils import get_redis
from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

import time


class event_handler(Core):
    @staticmethod
    def operation(session, meta, **options):
        return Operation.NO_OP

    @staticmethod
    def pre_job(hdlr_mod, logger, meta):
        job_name = meta["job_name"]
        config = meta["config"]
        r = get_redis(config)

        t0 = time.time()
        t0_key_handle = redis_key.redis_key_handle(r, "t0", job_name)
        t0_key_handle.set_value(t0)

    @staticmethod
    def post_job(hdlr_mod, logger, meta):
        job_name = meta["job_name"]
        config = meta["config"]
        t1 = time.time()
        r = get_redis(config)
        t0_key_handle = redis_key.redis_key_handle(r, "t0", job_name)
        t0 = t0_key_handle.get_value()
        t0_key_handle.reset()
        failures = redis_key.failures_key_handle(r, job_name)
        retries = redis_key.retries_key_handle(r, job_name)
        logger.info(
            "post_job",
            job_name=job_name,
            failures=failures.get_value(),
            retries=retries.get_value(),
            time_elasped=t1 - t0,
        )
