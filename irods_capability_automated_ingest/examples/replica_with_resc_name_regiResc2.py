from irods_capability_automated_ingest.core import Core

class event_handler(Core):

    @staticmethod
    def put(session, target, path, **options):
        return False
    
    @staticmethod
    def as_replica(session, target, path, **options):
        return True

    @staticmethod
    def to_resource(session, target, path, **options):
        return "regiResc2"
    



