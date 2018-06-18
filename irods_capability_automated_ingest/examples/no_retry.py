from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation
from redis import StrictRedis

class event_handler(Core):

    @staticmethod
    def operation(session, meta, **options):
        return Operation.NO_OP

    @staticmethod
    def pre_data_obj_create(hdlr_mod, logger, session, meta, **options):
        target = meta["target"]
        path = meta["path"]

        r = StrictRedis()
        failures = r.get("failures:"+path)
        if failures is None:
            failures = 0

        r.incr("failures:"+path)

        if failures == 0:
            raise RuntimeError("no failures")





