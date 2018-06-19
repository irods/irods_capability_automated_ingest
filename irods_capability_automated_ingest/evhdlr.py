from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):

    @staticmethod
    def post_data_obj_create(hdlr_mod, logger, session, meta, **options):
        target = meta["target"]
        print("create", target)

    @staticmethod
    def post_data_obj_modify(hdlr_mod, logger, session, meta, **options):
        target = meta["target"]
        print("modify", target)

    @staticmethod
    def post_coll_create(hdlr_mod, logger, session, meta, **options):
        target = meta["target"]
        print("create coll ", target)

    @staticmethod
    def to_resource(session, meta, **options):
        return "demoResc"

    @staticmethod
    def target_path(session, meta, **options):
        path = meta["path"]
        return path

    @staticmethod
    def as_user(meta, **options):
        return "tempZone", "rods"

    @staticmethod
    def operation(session, meta, **options):
        return Operation.REGISTER_SYNC

    @staticmethod
    def delay(hdlr_mod, logger, meta, retries):
        return 180
