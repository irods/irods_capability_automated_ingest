import os

from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

OPERATION = Operation.REGISTER_SYNC

class event_handler(Core):
    @staticmethod
    def operation(session, meta, **options):
        return OPERATION

    @staticmethod
    def pre_data_obj_modify(hdlr_mod, logger, session, meta, *args, **options):
        created_data_object_path = meta["target"]
        parent_collection_of_created_data_object = '/'.join(created_data_object_path.split('/')[:-1])

        attribute = "pre_data_obj_modify"
        value = created_data_object_path
        unit = OPERATION.name

        coll = session.collections.get(parent_collection_of_created_data_object)
        coll.metadata.add(attribute, value, unit)

    @staticmethod
    def post_data_obj_modify(hdlr_mod, logger, session, meta, *args, **options):
        created_data_object_path = meta["target"]
        parent_collection_of_created_data_object = '/'.join(created_data_object_path.split('/')[:-1])

        attribute = "post_data_obj_modify"
        value = created_data_object_path
        unit = OPERATION.name

        coll = session.collections.get(parent_collection_of_created_data_object)
        coll.metadata.add(attribute, value, unit)
