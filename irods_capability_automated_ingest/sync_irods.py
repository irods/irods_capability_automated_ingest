import io
import os
from os.path import dirname, basename
from irods.session import iRODSSession
from irods.models import Resource, DataObject, Collection
from irods.exception import NetworkException
from .sync_utils import get_redis
from .utils import Operation
from .custom_event_handler import custom_event_handler
import redis_lock
import json
import irods.keywords as kw
import base64
import ssl
import threading


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
                    meta2["target"] = dirname(target)
                    meta2["path"] = dirname(path)
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


def register_file(hdlr_mod, logger, session, meta, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    b64_path_str = meta.get("b64_path_str")

    event_handler = custom_event_handler(meta)
    if event_handler is None:
        phypath_to_register_in_catalog = None
    else:
        phypath_to_register_in_catalog = event_handler.target_path(session, **options)
    if phypath_to_register_in_catalog is None:
        if b64_path_str is not None and "unicode_error_filename" in meta:
            phypath_to_register_in_catalog = os.path.join(
                source_physical_fullpath, meta["unicode_error_filename"]
            )
        else:
            phypath_to_register_in_catalog = source_physical_fullpath

    resc_name = event_handler.to_resource(session, **options)
    if resc_name is not None:
        options["destRescName"] = resc_name

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    options[kw.DATA_SIZE_KW] = str(meta["size"])
    options[kw.DATA_MODIFY_KW] = str(int(meta["mtime"]))

    logger.info(
        "registering object {}, source_physical_fullpath: {}, options = {}".format(
            dest_dataobj_logical_fullpath, source_physical_fullpath, options
        )
    )
    session.data_objects.register(
        phypath_to_register_in_catalog, dest_dataobj_logical_fullpath, **options
    )

    logger.info("succeeded", task="irods_register_file", path=source_physical_fullpath)

    annotate_metadata_for_special_data_objs(
        meta, session, source_physical_fullpath, dest_dataobj_logical_fullpath
    )


def upload_file(hdlr_mod, logger, session, meta, scanner, op, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    b64_path_str = meta.get("b64_path_str")
    event_handler = custom_event_handler(meta)
    resc_name = event_handler.to_resource(session, **options)
    if resc_name is not None:
        options["destRescName"] = resc_name

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    # Use scanner object to upload files
    # exists=False because upload_file is only called from sync_data_from_file if file does not exist yet
    scanner.upload_file(logger, session, meta, op, exists=False, **options)
    annotate_metadata_for_special_data_objs(
        meta, session, source_physical_fullpath, dest_dataobj_logical_fullpath
    )


def no_op(hdlr_mod, logger, session, meta, **options):
    pass


def sync_file(hdlr_mod, logger, session, meta, scanner, op, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    b64_path_str = meta.get("b64_path_str")

    event_handler = custom_event_handler(meta)
    resc_name = event_handler.to_resource(session, **options)
    if resc_name is not None:
        options["destRescName"] = resc_name

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    logger.info("syncing object %s, options = %s" % (dest_dataobj_logical_fullpath, options))

    # TODO: Issue #208
    # Investigate behavior of sync_file when op is None
    if op is None:
        op = Operation.REGISTER_SYNC

    # Use scanner object to sync files
    # Ensure exists=True so that upload_file understands that it is a sync_file operation
    scanner.upload_file(logger, session, meta, op, exists=True, **options)


def update_metadata(hdlr_mod, logger, session, meta, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    event_handler = custom_event_handler(meta)
    phypath_to_register_in_catalog = event_handler.target_path(session, **options)
    b64_path_str = meta.get("b64_path_str")
    if phypath_to_register_in_catalog is None:
        if b64_path_str is not None and "unicode_error_filename" in meta:
            # Append generated filename to truncated fullpath because it failed to encode
            phypath_to_register_in_catalog = os.path.join(
                source_physical_fullpath, meta["unicode_error_filename"]
            )
        else:
            phypath_to_register_in_catalog = source_physical_fullpath

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    size = int(meta["size"])
    mtime = int(meta["mtime"])
    logger.info(
        f"updating object: {dest_dataobj_logical_fullpath}, options = {options}"
    )

    data_obj_info = {"objPath": dest_dataobj_logical_fullpath}

    outdated_repl_nums = []
    found = False

    resc_name = event_handler.to_resource(session, **options)
    if resc_name is None:
        found = True
    else:
        for row in session.query(
            Resource.name, DataObject.path, DataObject.replica_number
        ).filter(
            DataObject.name == basename(dest_dataobj_logical_fullpath),
            Collection.name == dirname(dest_dataobj_logical_fullpath),
        ):
            if row[DataObject.path] == phypath_to_register_in_catalog:
                if child_of(session, row[Resource.name], resc_name):
                    found = True
                    repl_num = row[DataObject.replica_number]
                    data_obj_info["replNum"] = repl_num
                    continue

    if not found:
        if b64_path_str is not None:
            logger.error(
                "updating object: wrong resource or path, "
                "dest_dataobj_logical_fullpath = {}, phypath_to_register_in_catalog = {}, options = {}".format(
                    dest_dataobj_logical_fullpath,
                    phypath_to_register_in_catalog,
                    str(options),
                )
            )
        else:
            logger.error(
                "updating object: wrong resource or path, "
                "dest_dataobj_logical_fullpath = {}, source_physical_fullpath = {}, "
                "phypath_to_register_in_catalog = {}, options = {}".format(
                    dest_dataobj_logical_fullpath,
                    source_physical_fullpath,
                    phypath_to_register_in_catalog,
                    str(options),
                )
            )
        raise Exception("wrong resource or path")

    session.data_objects.modDataObjMeta(
        data_obj_info,
        {"dataSize": size, "dataModify": mtime, "allReplStatus": 1},
        **options,
    )

    if b64_path_str is not None:
        logger.info(
            "succeeded",
            task="irods_update_metadata",
            path=phypath_to_register_in_catalog,
        )
    else:
        logger.info(
            "succeeded", task="irods_update_metadata", path=source_physical_fullpath
        )


def sync_file_meta(hdlr_mod, logger, session, meta, **options):
    pass


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


def sync_data_from_file(hdlr_mod, meta, logger, scanner, content, **options):
    target = meta["target"]
    path = meta["path"]
    init = meta["initial_ingest"]

    event_handler = custom_event_handler(meta)
    session = irods_session(event_handler.get_module(), meta, logger, **options)

    if init:
        exists = False

    else:
        if session.data_objects.exists(target):
            exists = True
        elif session.collections.exists(target):
            raise Exception(
                "sync: cannot sync file " + path + " to collection " + target
            )
        else:
            exists = False

    op = event_handler.operation(session, **options)

    if op == Operation.NO_OP:
        if not exists:
            event_handler.call(
                "on_data_obj_create", logger, no_op, logger, session, meta, **options
            )
        else:
            event_handler.call(
                "on_data_obj_modify", logger, no_op, logger, session, meta, **options
            )
    else:
        if op is None:
            op = Operation.REGISTER_SYNC
        createRepl = False
        if exists and op == Operation.REGISTER_AS_REPLICA_SYNC:
            resc_name = event_handler.to_resource(session, **options)
            if resc_name is None:
                raise Exception("no resource name defined")

            found = False
            foundPath = False
            for replica in session.data_objects.get(target).replicas:
                if child_of(session, replica.resource_name, resc_name):
                    found = True
                    if replica.path == path:
                        foundPath = True
            if found:
                if not foundPath:
                    raise Exception(
                        "there is at least one replica under resource but all replicas have wrong paths"
                    )
            else:
                createRepl = True

        put = op in [Operation.PUT, Operation.PUT_SYNC, Operation.PUT_APPEND]

        if not exists:
            meta2 = meta.copy()
            meta2["target"] = dirname(target)
            if "b64_path_str" not in meta2:
                meta2["path"] = dirname(path)
            create_dirs(logger, session, meta2, **options)
            if put:
                event_handler.call(
                    "on_data_obj_create",
                    logger,
                    upload_file,
                    logger,
                    session,
                    meta,
                    scanner,
                    op,
                    **options,
                )
            else:
                event_handler.call(
                    "on_data_obj_create",
                    logger,
                    register_file,
                    logger,
                    session,
                    meta,
                    **options,
                )
        elif createRepl:
            options["regRepl"] = ""

            event_handler.call(
                "on_data_obj_create",
                logger,
                register_file,
                logger,
                session,
                meta,
                **options,
            )
        elif content:
            if put:
                sync = op in [Operation.PUT_SYNC, Operation.PUT_APPEND]
                if sync:
                    event_handler.call(
                        "on_data_obj_modify",
                        logger,
                        sync_file,
                        logger,
                        session,
                        meta,
                        scanner,
                        op,
                        **options,
                    )
            else:
                event_handler.call(
                    "on_data_obj_modify",
                    logger,
                    update_metadata,
                    logger,
                    session,
                    meta,
                    **options,
                )
        else:
            event_handler.call(
                "on_data_obj_modify",
                logger,
                sync_file_meta,
                logger,
                session,
                meta,
                **options,
            )

    start_timer()


def sync_metadata_from_file(hdlr_mod, meta, logger, scanner, **options):
    sync_data_from_file(hdlr_mod, meta, logger, scanner, False, **options)


def sync_dir_meta(hdlr_mod, logger, session, meta, **options):
    pass


def sync_data_from_dir(hdlr_mod, meta, logger, scanner, content, **options):
    target = meta["target"]
    path = meta["path"]

    event_handler = custom_event_handler(meta)
    session = irods_session(event_handler.get_module(), meta, logger, **options)
    exists = session.collections.exists(target)

    op = event_handler.operation(session, **options)
    if op == Operation.NO_OP:
        if not exists:
            event_handler.call(
                "on_coll_create", logger, no_op, logger, session, meta, **options
            )
        else:
            event_handler.call(
                "on_coll_modify", logger, no_op, logger, session, meta, **options
            )
    else:
        if op is None:
            op = Operation.REGISTER_SYNC

        if not exists:
            create_dirs(logger, session, meta, **options)
        else:
            event_handler.call(
                "on_coll_modify",
                logger,
                sync_dir_meta,
                logger,
                session,
                meta,
                **options,
            )
    start_timer()


def sync_metadata_from_dir(hdlr_mod, meta, logger, scanner, **options):
    sync_data_from_dir(hdlr_mod, meta, logger, scanner, False, **options)
