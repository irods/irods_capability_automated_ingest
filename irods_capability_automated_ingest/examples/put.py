from irods_capability_automated_ingest.core import Core

class event_handler(Core):
    
    @staticmethod
    def put(session, target, path, **options):
        return True


