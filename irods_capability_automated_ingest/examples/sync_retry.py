from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):

    @staticmethod
    def max_retries(hdlr_mod, logger, target, path):
        return 3

    @staticmethod
    def operation(session, target, path, **options):
        return Operation.PUT_SYNC

