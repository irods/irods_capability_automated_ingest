from core import Core

class event_handler(Core):
    
    @staticmethod
    def to_leaf_resource(session, target, path, **options):
        return "regiResc"

    @staticmethod
    def as_replica(session, target, path, **options):
        return False

    @staticmethod
    def put(session, target, path, **options):
        return False

