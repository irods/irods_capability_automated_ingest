from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation
from irods.meta import iRODSMeta
import os

filesystem_mode = 'filesystem::mode'

class event_handler(Core):

    @staticmethod
    def post_data_obj_create(hdlr_mod, logger, session, meta, **options):
        target = meta["target"]
        path = meta["path"]
        s = os.stat(path)
        mode = s.st_mode

        obj = session.data_objects.get(target)
        obj.metadata.add(filesystem_mode, str(mode), '')


    @staticmethod
    def post_data_obj_modify(hdlr_mod, logger, session, meta, **options):
        target = meta["target"]
        path = meta["path"]
        s = os.stat(path)
        mode = s.st_mode
        obj = session.data_objects.get(target)
        obj.metadata[filesystem_mode] = iRODSMeta(filesystem_mode, str(mode))

    @staticmethod
    def operation(session, meta, **options):
        return Operation.REGISTER



