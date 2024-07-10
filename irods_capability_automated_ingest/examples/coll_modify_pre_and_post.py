import os

from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

OPERATION = Operation.REGISTER_SYNC

class event_handler(Core):
    @staticmethod
    def operation(session, meta, **options):
        return OPERATION

    @staticmethod
    def pre_coll_modify(hdlr_mod, logger, session, meta, *args, **options):
        modified_collection = meta["target"]

        attribute = "pre_coll_modify"
        value = meta['job_name']
        unit = OPERATION.name

        coll = session.collections.get(modified_collection)
        coll.metadata.add(attribute, value, unit)

    @staticmethod
    def post_coll_modify(hdlr_mod, logger, session, meta, *args, **options):
        modified_collection = meta["target"]

        attribute = "post_coll_modify"
        value = meta['job_name']
        unit = OPERATION.name

        coll = session.collections.get(modified_collection)
        coll.metadata.add(attribute, value, unit)
