from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation


class event_handler(Core):
    @staticmethod
    def operation(session, meta, **options):
        return Operation.NO_OP

    @staticmethod
    def post_job(hdlr_mod, logger, meta):
        # Amend here for testing so that we can ensure that post_job executes once per job.
        with open("/tmp/a", "a") as f:
            f.write("post_job")
