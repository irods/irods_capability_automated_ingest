from core import Core

class event_handler(Core):
    
    @staticmethod
    def sync(session, target, path, **options):
        return False

    @staticmethod
    def put(session, target, path, **options):
        return True

