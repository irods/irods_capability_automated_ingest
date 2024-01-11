from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation
import re


class event_handler(Core):
    re_non_alphanum = re.compile("[^a-zA-Z0-9]")

    @staticmethod
    def character_map():
        return {
            event_handler.re_non_alphanum: "_"
        }  # map any non-ascii or non-alphanumeric
        # character to '_'

    @staticmethod
    def operation(session, meta, **options):
        return Operation.REGISTER_SYNC
