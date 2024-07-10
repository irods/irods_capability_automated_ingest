import os

from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

OPERATION = Operation.REGISTER_SYNC

class event_handler(Core):
    @staticmethod
    def operation(session, meta, **options):
        return OPERATION

    @staticmethod
    def pre_coll_create(hdlr_mod, logger, session, meta, *args, **options):
        created_collection = meta["target"]
        parent_of_created_collection = '/'.join(created_collection.split('/')[:-1])

        attribute = "pre_coll_create"
        value = created_collection
        unit = OPERATION.name

        coll = session.collections.get(parent_of_created_collection)
        coll.metadata.add(attribute, value, unit)

    @staticmethod
    def post_coll_create(hdlr_mod, logger, session, meta, *args, **options):
        created_collection = meta["target"]
        parent_of_created_collection = '/'.join(created_collection.split('/')[:-1])

        attribute = "post_coll_create"
        value = created_collection
        unit = OPERATION.name

        coll = session.collections.get(parent_of_created_collection)
        coll.metadata.add(attribute, value, unit)
