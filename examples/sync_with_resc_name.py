from core import Core

class event_handler(Core):
    
    @staticmethod
    def to_root_resource(session, target, path, **options):
        return "putResc"

    @staticmethod
    def put(session, target, path, **options):
        return True

    @staticmethod
    def sync(session, target, path, **options):
        return True

