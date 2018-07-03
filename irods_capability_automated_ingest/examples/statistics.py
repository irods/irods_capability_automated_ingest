from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation
import time
from ..sync_utils import get_redis, get_with_key, failures_key, retries_key, set_with_key, reset_with_key


def t0_key(a):
    return "t0:/" + a


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
        set_with_key(r, t0_key, job_name, t0)

    @staticmethod
    def post_job(hdlr_mod, logger, meta):
        job_name = meta["job_name"]
        config = meta["config"]
        t1 = time.time()
        r = get_redis(config)
        t0 = get_with_key(r, t0_key, job_name, float)
        reset_with_key(r, t0_key, job_name)
        failures = get_with_key(r, failures_key, job_name, int)
        retries = get_with_key(r, retries_key, job_name, int)
        logger.info("post_job", job_name=job_name, failures=failures, retries=retries, time_elasped=t1 - t0)
