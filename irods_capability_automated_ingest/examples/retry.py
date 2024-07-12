from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation
from irods_capability_automated_ingest.sync_utils import get_redis


class event_handler(Core):
    @staticmethod
    def max_retries(hdlr_mod, logger, meta):
        return 3

    @staticmethod
    def delay(hdlr_mod, logger, meta, retries):
        return 0

    @staticmethod
    def operation(session, meta, **options):
        return Operation.NO_OP

    @staticmethod
    def pre_data_obj_create(hdlr_mod, logger, session, meta, *args, **options):
        path = meta["path"]
        target = meta["target"]

        r = get_redis(meta["config"])
        failures = r.get("failures:" + path)
        if failures is None:
            failures = 0

        r.incr("failures:" + path)

        if failures == 0:
            raise RuntimeError("no failures")
