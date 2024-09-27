from .. import custom_event_handler, sync_logging
from ..redis_utils import get_redis
from ..utils import DeleteMode, Operation

from irods.exception import CollectionDoesNotExist, NetworkException
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
    if all(
        [
            env_irods_host,
            env_irods_port,
            env_irods_user_name,
            env_irods_zone_name,
            env_irods_password,
        ]
    ):
        kwargs["host"] = env_irods_host
        kwargs["port"] = env_irods_port
        kwargs["user"] = env_irods_user_name
        kwargs["zone"] = env_irods_zone_name
        kwargs["password"] = env_irods_password
    else:
        if not env_file:
            # TODO(#250): This will not work on Windows.
            env_file = os.path.expanduser("~/.irods/irods_environment.json")

        kwargs["irods_env_file"] = env_file

    if hasattr(handler_module, "as_user"):
        client_zone, client_user = handler_module.as_user(meta, **options)
        kwargs["client_user"] = client_user
        kwargs["client_zone"] = client_zone

    key = json.dumps(kwargs)  # todo add timestamp of env file to key

    if env_file:
        if not os.path.exists(env_file):
            raise FileNotFoundError(
                f"Specified iRODS client environment file [{env_file}] does not exist."
            )

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

    if key in irods_session_map:
        sess = irods_session_map.get(key)
    else:
        # TODO: #42 - pull out 10 into configuration
        for i in range(10):
            try:
                sess = iRODSSession(**kwargs)
                irods_session_map[key] = sess
                break
            except NetworkException:
                time.sleep(0.1)

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
    event_handler = custom_event_handler.custom_event_handler(meta)
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


def list_collection(meta, logger, logical_path):
    event_handler = custom_event_handler.custom_event_handler(meta)
    session = irods_session(event_handler.get_module(), meta, logger, **dict())

    collection = session.collections.get(logical_path)

    return collection.subcollections, collection.data_objects


def unregister_data_object(hdlr_mod, session, meta, **options):
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)
    logger.debug(f"calling unregister for [{meta['target']}]")
    session.data_objects.unregister(meta["target"], **options)


def trash_data_object(hdlr_mod, session, meta, **options):
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)
    logger.debug(f"calling unlink (trash) for [{meta['target']}]")
    session.data_objects.unlink(meta["target"], **options)


def unlink_data_object(hdlr_mod, session, meta, **options):
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)
    logger.debug(f"calling unlink (no trash / force=True) for [{meta['target']}]")
    session.data_objects.unlink(meta["target"], force=True, **options)


def get_delete_function(delete_mode):
    delete_mode_to_function = {
        DeleteMode.DO_NOT_DELETE: None,
        DeleteMode.UNREGISTER: unregister_data_object,
        DeleteMode.TRASH: trash_data_object,
        DeleteMode.NO_TRASH: unlink_data_object,
    }
    return delete_mode_to_function.get(delete_mode, None)


def delete_data_object(hdlr_mod, meta, **options):
    logical_path = meta["target"]

    event_handler = custom_event_handler.custom_event_handler(meta)

    delete_mode = event_handler.delete_mode()
    if DeleteMode.DO_NOT_DELETE == delete_mode:
        # The event handler says "do not delete", so do not delete.
        return

    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)

    session = irods_session(event_handler.get_module(), meta, logger, **options)

    if not session.data_objects.exists(logical_path):
        # There is nothing to do if the data object does not exist.
        return

    delete_function = get_delete_function(delete_mode)
    if delete_function is None:
        raise RuntimeError(f"delete_mode [{delete_mode}] is not supported")

    event_handler.call(
        "on_data_obj_delete", logger, delete_function, session, meta, **options
    )


def unregister_collection(hdlr_mod, session, meta, **options):
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)
    logger.debug(f"calling unregister for [{meta['target']}]")
    # We should only be removing an empty collection, so explicitly do not remove recursively or do a "force" remove.
    options["recurse"] = False
    options["force"] = False
    session.collections.unregister(meta["target"], **options)


def delete_collection(hdlr_mod, meta, **options):
    logical_path = meta["target"]

    event_handler = custom_event_handler.custom_event_handler(meta)

    delete_mode = event_handler.delete_mode()
    if DeleteMode.DO_NOT_DELETE == delete_mode:
        # The event handler says "do not delete", so do not delete.
        return

    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)

    session = irods_session(event_handler.get_module(), meta, logger, **options)

    r = get_redis(config)
    with redis_lock.Lock(r, "delete_collection:" + logical_path):
        # This will raise CollectionDoesNotExist if logical_path does not exist.
        collection = session.collections.get(logical_path)

        if 0 != len(collection.data_objects) or 0 != len(collection.subcollections):
            logger.debug(
                f"Collection [{logical_path}] is not empty and will not be removed."
            )
            return

        event_handler.call(
            "on_coll_delete", logger, unregister_collection, session, meta, **options
        )

    # Attempt to remove the parent collection if it is found to be empty.
    root_target_collection = meta["root_target_collection"]
    parent_collection = "/".join(logical_path.split("/")[:-1])
    if parent_collection == root_target_collection:
        logger.info(f"Cannot remove root target collection [{root_target_collection}]")
        return
    with redis_lock.Lock(r, "delete_collection:" + parent_collection):
        # This will raise CollectionDoesNotExist if logical_path does not exist.
        collection = session.collections.get(parent_collection)
        if 0 != len(collection.data_objects) or 0 != len(collection.subcollections):
            logger.debug(
                f"Collection [{parent_collection}] is not empty and will not be removed."
            )
            return
        event_handler.call(
            "on_coll_delete", logger, unregister_collection, session, meta, **options
        )
