from ..custom_event_handler import custom_event_handler
from ..redis_utils import get_redis
from ..utils import Operation

from irods.exception import NetworkException
from irods.models import Collection, DataObject, Resource
from irods.session import iRODSSession

import base64
import json
import os
import redis_lock
import ssl
import threading

irods_session_map = {}
irods_session_timer_map = {}


class disconnect_timer(object):
    def __init__(self, logger, interval, sess_map):
        self.logger = logger
        self.interval = interval
        self.timer = None
        self.sess_map = sess_map

    def callback(self):
        for k, v in self.sess_map.items():
            self.logger.info("Cleaning up session [" + k + "]")
            v.cleanup()
        self.sess_map.clear()

    def cancel(self):
        if self.timer is not None:
            self.timer.cancel()

    def start(self):
        self.timer = threading.Timer(self.interval, self.callback)
        self.timer.start()


def stop_timer():
    for k, v in irods_session_timer_map.items():
        v.cancel()


def start_timer():
    for k, v in irods_session_timer_map.items():
        v.start()


def irods_session(handler_module, meta, logger, **options):
    env_irods_host = os.environ.get("IRODS_HOST")
    env_irods_port = os.environ.get("IRODS_PORT")
    env_irods_user_name = os.environ.get("IRODS_USER_NAME")
    env_irods_zone_name = os.environ.get("IRODS_ZONE_NAME")
    env_irods_password = os.environ.get("IRODS_PASSWORD")

    env_file = os.environ.get("IRODS_ENVIRONMENT_FILE")

    kwargs = {}
    if (
        env_irods_host is None
        or env_irods_port is None
        or env_irods_user_name is None
        or env_irods_zone_name is None
        or env_irods_password is None
    ):
        if env_file is None:
            # TODO(#250): This will not work on Windows.
            env_file = os.path.expanduser("~/.irods/irods_environment.json")

        kwargs["irods_env_file"] = env_file
    else:
        kwargs["host"] = env_irods_host
        kwargs["port"] = env_irods_port
        kwargs["user"] = env_irods_user_name
        kwargs["zone"] = env_irods_zone_name
        kwargs["password"] = env_irods_password

    if hasattr(handler_module, "as_user"):
        client_zone, client_user = handler_module.as_user(meta, **options)
        kwargs["client_user"] = client_user
        kwargs["client_zone"] = client_zone

    key = json.dumps(kwargs)  # todo add timestamp of env file to key

    if env_file:
        with open(env_file) as irods_env:
            irods_env_as_json = json.load(irods_env)
            verify_server = irods_env_as_json.get("irods_ssl_verify_server")
            ca_file = irods_env_as_json.get("irods_ssl_ca_certificate_file")
            if verify_server and verify_server != "none" and ca_file:
                kwargs["ssl_context"] = ssl.create_default_context(
                    purpose=ssl.Purpose.SERVER_AUTH,
                    cafile=ca_file,
                    capath=None,
                    cadata=None,
                )

    if not key in irods_session_map:
        # TODO: #42 - pull out 10 into configuration
        for i in range(10):
            try:
                sess = iRODSSession(**kwargs)
                irods_session_map[key] = sess
                break
            except NetworkException:
                if i < 10:
                    time.sleep(0.1)
                else:
                    raise
    else:
        sess = irods_session_map.get(key)

    # =-=-=-=-=-=-=-
    # disconnect timer
    if key in irods_session_timer_map:
        timer = irods_session_timer_map[key]
        timer.cancel()
        irods_session_timer_map.pop(key, None)
    idle_sec = meta["idle_disconnect_seconds"]
    logger.info("iRODS Idle Time set to: " + str(idle_sec))

    timer = disconnect_timer(logger, idle_sec, irods_session_map)
    irods_session_timer_map[key] = timer
    # =-=-=-=-=-=-=-

    return sess


def validate_target_collection(meta, logger):
    # root cannot be the target collection
    destination_collection_logical_path = meta["target"]
    if destination_collection_logical_path == "/":
        raise Exception("Root may only contain collections which represent zones")


def child_of(session, child_resc_name, resc_name):
    if child_resc_name == resc_name:
        return True
    else:
        while True:
            child_resc = session.resources.get(child_resc_name)
            parent_resc_id = child_resc.parent
            if parent_resc_id is None:
                break

            parent_resc_name = None
            for row in session.query(Resource.name).filter(
                Resource.id == parent_resc_id
            ):
                parent_resc_name = row[Resource.name]
            if parent_resc_name == resc_name:
                return True
            child_resc_name = parent_resc_name
        return False


def create_dirs(logger, session, meta, **options):
    target = meta["target"]
    path = meta["path"]
    config = meta["config"]
    event_handler = custom_event_handler(meta)
    if target.startswith("/"):
        r = get_redis(config)
        if not session.collections.exists(target):
            with redis_lock.Lock(r, "create_dirs:" + path):
                if not session.collections.exists(target):
                    meta2 = meta.copy()
                    # TODO(#250): This will not work on Windows.
                    meta2["target"] = os.path.dirname(target)
                    meta2["path"] = os.path.dirname(path)
                    # TODO: Does this need to happen after the create call?
                    create_dirs(logger, session, meta2, **options)

                    event_handler.call(
                        "on_coll_create",
                        logger,
                        create_dir,
                        logger,
                        session,
                        meta,
                        **options,
                    )
    else:
        raise Exception(
            "create_dirs: relative path; target:[" + target + "]; path:[" + path + "]"
        )


def create_dir(hdlr_mod, logger, session, meta, **options):
    target = meta["target"]
    path = meta["path"]
    logger.info("creating collection " + target)
    session.collections.create(target)


def annotate_metadata_for_special_data_objs(
    meta, session, source_physical_fullpath, dest_dataobj_logical_fullpath
):
    def add_metadata_if_not_present(obj, key, val, unit=None):
        # TODO: If updating/syncing link items, we might want to update the readlink result...
        if key not in obj.metadata.keys():
            obj.metadata.add(key, val, unit)

    b64_path_str = meta.get("b64_path_str") or meta.get("b64_path_str_charmap")
    if b64_path_str is not None:
        b64_reason = meta.get("b64_reason")
        if b64_reason in ("UnicodeEncodeError", "character_map"):
            add_metadata_if_not_present(
                session.data_objects.get(dest_dataobj_logical_fullpath),
                "irods::automated_ingest::{}".format(b64_reason),
                b64_path_str,
                "python3.base64.b64encode(full_path_of_source_file)",
            )

    if meta["is_socket"]:
        add_metadata_if_not_present(
            session.data_objects.get(dest_dataobj_logical_fullpath),
            "socket_target",
            "socket",
            "automated_ingest",
        )
    elif meta["is_link"]:
        add_metadata_if_not_present(
            session.data_objects.get(dest_dataobj_logical_fullpath),
            "link_target",
            os.path.join(
                os.path.dirname(source_physical_fullpath),
                os.readlink(source_physical_fullpath),
            ),
            "automated_ingest",
        )


def size(session, path, replica_num=None, resc_name=None):
    args = [
        Collection.name == os.path.dirname(path),
        DataObject.name == os.path.basename(path),
    ]

    if replica_num is not None:
        args.append(DataObject.replica_number == replica_num)

    if resc_name is not None:
        args.append(DataObject.resource_name == resc_name)

    for row in session.query(DataObject.size).filter(*args):
        return int(row[DataObject.size])
