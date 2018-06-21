from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):

    @staticmethod
    def operation(session, meta, **options):
        return Operation.NO_OP

    @staticmethod
    def pre_data_obj_create(hdlr_mod, logger, session, meta, **options):

        if meta["append_json"] != "append_json":
            raise RuntimeError("append_json error")
