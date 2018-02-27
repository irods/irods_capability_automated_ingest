from core import Core

class event_handler(Core):
    
    @staticmethod
    def to_resource(session, target, path, **options):
        return "regiResc2"

    @staticmethod
    def as_replica(session, target, path, **options):
        return False

    @staticmethod
    def put(session, target, path, **options):
        return False

