from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):
    @staticmethod
    def operation(session, meta, **options):
        return Operation.REGISTER_SYNC

    @staticmethod
    def pre_data_obj_create(hdlr_mod, logger, session, meta, **options):
        logical_path = meta['target']
        logger.info('pre_data_obj_create:['+logical_path+']')

    @staticmethod
    def post_data_obj_create(hdlr_mod, logger, session, meta, **options):
        logical_path = meta['target']
        logger.info('post_data_obj_create:['+logical_path+']')

    @staticmethod
    def pre_coll_create(hdlr_mod, logger, session, meta, **options):
        logical_path = meta['target']
        logger.info('pre_coll_create:['+logical_path+']')

    @staticmethod
    def post_coll_create(hdlr_mod, logger, session, meta, **options):
        logical_path = meta['target']
        logger.info('post_coll_create:['+logical_path+']')

    @staticmethod
    def pre_data_obj_modify(hdlr_mod, logger, session, meta, **options):
        logical_path = meta['target']
        logger.info('pre_data_obj_modify:['+logical_path+']')

    @staticmethod
    def post_data_obj_modify(hdlr_mod, logger, session, meta, **options):
        logical_path = meta['target']
        logger.info('post_data_obj_modify:['+logical_path+']')

    @staticmethod
    def pre_coll_modify(hdlr_mod, logger, session, meta, **options):
        logical_path = meta['target']
        logger.info('pre_coll_modify:['+logical_path+']')

    @staticmethod
    def post_coll_modify(hdlr_mod, logger, session, meta, **options):
        logical_path = meta['target']
        logger.info('post_coll_modify:['+logical_path+']')
