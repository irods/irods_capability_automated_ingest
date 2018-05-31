from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):

    @staticmethod
    def to_resource(session, target, path, **options):
        return "regiResc2Root"

    @staticmethod
    def operation(session, target, path, **options):
        return Operation.PUT_APPEND

