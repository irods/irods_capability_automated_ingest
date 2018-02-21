from core import Core

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
    def to_root_resource(session, target, path, **options):
        return "demoResc"

    @staticmethod
    def to_leaf_resource(session, target, path, **options):
        return "demoResc"

    @staticmethod
    def to_resource_hier(session, target, path, **options):
        return "demoResc"

    @staticmethod
    def as_user(target, path, **options):
        return "tempZone", "rods"

    @staticmethod
    def as_replica(session, target, path, **options):
        return False

    @staticmethod
    def put(session, target, path, **options):
        return False

    @staticmethod
    def sync(session, target, path, **options):
        return True

