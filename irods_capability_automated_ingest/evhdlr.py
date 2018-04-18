from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):
    
    @staticmethod
    def post_data_obj_create(hdlr_mod, session, target, path, **options):
        print("create", target)

    @staticmethod
    def post_data_obj_modify(hdlr_mod, session, target, path, **options):
        print("modify", target)

    @staticmethod
    def post_coll_create(hdlr_mod, session, target, path, **options):
        print("create coll ", target)

    @staticmethod
    def to_resource(session, target, path, **options):
        return "demoResc"

    @staticmethod
    def target_path(session, target, path, **options):
        return path

    @staticmethod
    def as_user(target, path, **options):
        return "tempZone", "rods"

    @staticmethod
    def operation(session, target, path, **options):
        return Operation.REGISTER_SYNC
