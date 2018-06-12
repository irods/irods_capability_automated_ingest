from rq import get_current_job
from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):

    @staticmethod
    def operation(session, target, path, **options):
        return Operation.NO_OP

    @staticmethod
    def pre_data_obj_create(hdlr_mod, logger, session, target, path, **options):
        job = get_current_job()
        failures = job.meta.get("failures")
        if failures is None:
            failures = 0

        if failures == 0:
            raise RuntimeError("no failures")





