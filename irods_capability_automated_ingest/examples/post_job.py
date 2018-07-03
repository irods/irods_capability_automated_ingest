from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation
from redis import StrictRedis

class event_handler(Core):

    @staticmethod
    def operation(session, meta, **options):
        return Operation.NO_OP

    @staticmethod
    def post_job(hdlr_mod, logger, meta):
        with open("/tmp/a", "w") as f:
            f.write("post_job")





