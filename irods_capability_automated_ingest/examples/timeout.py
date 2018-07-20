import time
from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):

    @staticmethod
    def operation(session, meta, **options):
        return Operation.NO_OP

    @staticmethod
    def timeout(hdlr_mod, logger, meta):
        return 1

    @staticmethod
    def pre_data_obj_create(hdlr_mod, logger, session, meta, **options):

        time.sleep(2)
