import os
from os.path import dirname, basename
from irods.session import iRODSSession
from irods.models import Resource, DataObject, Collection
from .sync_utils import size, get_redis, call, get_hdlr_mod
from .utils import Operation
import redis_lock
import json
import irods.keywords as kw
import base64, random
import threading

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
            for row in session.query(Resource.name).filter(Resource.id == parent_resc_id):
                parent_resc_name = row[Resource.name]
            if parent_resc_name == resc_name:
                return True
            child_resc_name = parent_resc_name
        return False


def create_dirs(hdlr_mod, logger, session, meta, **options):
    target = meta["target"]
    path = meta["path"]
    config = meta["config"]
    if target.startswith("/"):
        r = get_redis(config)
        if not session.collections.exists(target):
            with redis_lock.Lock(r, "create_dirs:" + path):
                if not session.collections.exists(target):
                    if target == "/":
                        raise Exception("create_dirs: Cannot create root")
                    meta2 = meta.copy()
                    meta2["target"] = dirname(target)
                    meta2["path"] = dirname(path)
                    create_dirs(hdlr_mod, logger, session, meta2, **options)

                    call(hdlr_mod, "on_coll_create", create_dir, logger, hdlr_mod, logger, session, meta, **options)
    else:
        raise Exception("create_dirs: relative path; target:[" + target + ']; path:[' + path + ']')


def create_dir(hdlr_mod, logger, session, meta, **options):
    target = meta["target"]
    path = meta["path"]
    logger.info("creating collection " + target)
    session.collections.create(target)


def get_target_path(hdlr_mod, session, meta, **options):
    if hasattr(hdlr_mod, "target_path"):
        return hdlr_mod.target_path(session, meta, **options)
    else:
        return None


def get_resource_name(hdlr_mod, session, meta, **options):
    if hasattr(hdlr_mod, "to_resource"):
        return hdlr_mod.to_resource(session, meta, **options)
    else:
        return None


def register_file(hdlr_mod, logger, session, meta, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    b64_path_str = meta.get('b64_path_str')

    phypath_to_register_in_catalog = get_target_path(hdlr_mod, session, meta, **options)
    if phypath_to_register_in_catalog is None:
        if b64_path_str is not None:
            phypath_to_register_in_catalog = os.path.join(source_physical_fullpath, meta['unicode_error_filename'])
        else:
            phypath_to_register_in_catalog = source_physical_fullpath

    resc_name = get_resource_name(hdlr_mod, session, meta, **options)

    if resc_name is not None:
        options["destRescName"] = resc_name

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    options[kw.DATA_SIZE_KW] = str(meta['size'])
    options[kw.DATA_MODIFY_KW] = str(int(meta['mtime']))

    logger.info("registering object " + dest_dataobj_logical_fullpath + ", options = " + str(options))
    session.data_objects.register(phypath_to_register_in_catalog, dest_dataobj_logical_fullpath, **options)

    logger.info("succeeded", task="irods_register_file", path = source_physical_fullpath)

    if b64_path_str is not None:
        obj = session.data_objects.get(meta['target'])
        obj.metadata.add("irods::automated_ingest::UnicodeEncodeError", meta['b64_path_str'], 'python3.base64.b64encode(full_path_of_source_file)')

    if meta['is_socket']:
        obj = session.data_objects.get(dest_dataobj_logical_fullpath)
        obj.metadata.add('socket_target', 'socket', 'automated_ingest')
    elif meta['is_link']:
        obj = session.data_objects.get(dest_dataobj_logical_fullpath)
        link_target = os.path.join(os.path.dirname(source_physical_fullpath), os.readlink(source_physical_fullpath))
        obj.metadata.add('link_target', link_target, 'automated_ingest')

def upload_file(hdlr_mod, logger, session, meta, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    resc_name = get_resource_name(hdlr_mod, session, meta, **options)

    if resc_name is not None:
        options["destRescName"] = resc_name

    logger.info("uploading object " + dest_dataobj_logical_fullpath + ", options = " + str(options))
    session.data_objects.put(source_physical_fullpath, dest_dataobj_logical_fullpath, **options)
    logger.info("succeeded", task="irods_upload_file", path = source_physical_fullpath)


def no_op(hdlr_mod, logger, session, meta, **options):
    pass


def sync_file(hdlr_mod, logger, session, meta, **options):
    target = meta["target"]
    path = meta["path"]
    logger.info("syncing object " + target + ", options = " + str(options))

    resc_name = get_resource_name(hdlr_mod, session, meta, **options)

    if resc_name is not None:
        options["destRescName"] = resc_name

    op = hdlr_mod.operation(session, meta, **options)

    if op == Operation.PUT_APPEND:
        BUFFER_SIZE = 1024
        logger.info("appending object " + target + ", options = " + str(options))
        tsize = size(session, target)
        tfd = session.data_objects.open(target, "a", **options)
        tfd.seek(tsize)
        with open(path, "rb") as sfd:
            sfd.seek(tsize)
            while True:
                buf = sfd.read(BUFFER_SIZE)
                if buf == b"":
                    break
                tfd.write(buf)
        tfd.close()
        logger.info("succeeded", task="irods_append_file", path=path)

    else:
        logger.info("uploading object " + target + ", options = " + str(options))
        session.data_objects.put(path, target, **options)
        logger.info("succeeded", task="irods_update_file", path = path)


def update_metadata(hdlr_mod, logger, session, meta, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    phypath_to_register_in_catalog = get_target_path(hdlr_mod, session, meta, **options)
    b64_path_str = meta.get('b64_path_str')
    if phypath_to_register_in_catalog is None:
        if b64_path_str is not None:
            # Append generated filename to truncated fullpath because it failed to encode
            phypath_to_register_in_catalog = os.path.join(source_physical_fullpath, meta['unicode_error_filename'])
        else:
            phypath_to_register_in_catalog = source_physical_fullpath

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    size = int(meta['size'])
    mtime = int(meta['mtime'])
    logger.info("updating object: " + dest_dataobj_logical_fullpath + ", options = " + str(options))

    data_obj_info = {"objPath": dest_dataobj_logical_fullpath}

    resc_name = get_resource_name(hdlr_mod, session, meta, **options)
    outdated_repl_nums = []
    found = False
    if resc_name is None:
        found = True
    else:
        for row in session.query(Resource.name, DataObject.path, DataObject.replica_number).filter(DataObject.name == basename(dest_dataobj_logical_fullpath), Collection.name == dirname(dest_dataobj_logical_fullpath)):
            if row[DataObject.path] == phypath_to_register_in_catalog:
                if child_of(session, row[Resource.name], resc_name):
                    found = True
                    repl_num = row[DataObject.replica_number]
                    data_obj_info["replNum"] = repl_num
                    continue

    if not found:
        if b64_path_str is not None:
            logger.error("updating object: wrong resource or path, dest_dataobj_logical_fullpath = " + dest_dataobj_logical_fullpath + ", phypath_to_register_in_catalog = " + phypath_to_register_in_catalog + ", phypath_to_register_in_catalog = " + phypath_to_register_in_catalog + ", options = " + str(options))
        else:
            logger.error("updating object: wrong resource or path, dest_dataobj_logical_fullpath = " + dest_dataobj_logical_fullpath + ", source_physical_fullpath = " + source_physical_fullpath + ", phypath_to_register_in_catalog = " + phypath_to_register_in_catalog + ", options = " + str(options))
        raise Exception("wrong resource or path")

    session.data_objects.modDataObjMeta(data_obj_info, {"dataSize":size, "dataModify":mtime, "allReplStatus":1}, **options)

    if b64_path_str is not None:
        logger.info("succeeded", task="irods_update_metadata", path = phypath_to_register_in_catalog)
    else:
        logger.info("succeeded", task="irods_update_metadata", path = source_physical_fullpath)


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
            self.logger.info('Cleaning up session ['+k+']')
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
        v.cancel();

def start_timer():
    for k, v in irods_session_timer_map.items():
        v.start();

def irods_session(hdlr_mod, meta, logger, **options):
    env_irods_host = os.environ.get("IRODS_HOST")
    env_irods_port = os.environ.get("IRODS_PORT")
    env_irods_user_name = os.environ.get("IRODS_USER_NAME")
    env_irods_zone_name = os.environ.get("IRODS_ZONE_NAME")
    env_irods_password = os.environ.get("IRODS_PASSWORD")

    env_file = os.environ.get('IRODS_ENVIRONMENT_FILE')

    kwargs = {}
    if env_irods_host is None or \
            env_irods_port is None or \
            env_irods_user_name is None or \
            env_irods_zone_name is None or \
            env_irods_password is None:
        if env_file is None:
            env_file = os.path.expanduser('~/.irods/irods_environment.json')

        kwargs["irods_env_file"] = env_file
    else:
        kwargs["host"] = env_irods_host
        kwargs["port"] = env_irods_port
        kwargs["user"] = env_irods_user_name
        kwargs["zone"] = env_irods_zone_name
        kwargs["password"] = env_irods_password

    if hasattr(hdlr_mod, "as_user"):
        client_zone, client_user = hdlr_mod.as_user(meta, **options)
        kwargs["client_user"] = client_user
        kwargs["client_zone"] = client_zone

    key = json.dumps(kwargs) # todo add timestamp of env file to key


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
    idle_sec = meta['idle_disconnect_seconds']
    logger.info("iRODS Idle Time set to: "+str(idle_sec))

    timer = disconnect_timer(logger, idle_sec, irods_session_map)
    irods_session_timer_map[key] = timer
    # =-=-=-=-=-=-=-

    return sess


def sync_data_from_file(meta, logger, content, **options):
    target = meta["target"]
    path = meta["path"]
    hdlr_mod = get_hdlr_mod(meta)
    init = meta["initial_ingest"]

    session = irods_session(hdlr_mod, meta, logger, **options)

    if init:
        exists = False
    else:
        if session.data_objects.exists(target):
            exists = True
        elif session.collections.exists(target):
            raise Exception("sync: cannot sync file " + path + " to collection " + target)
        else:
            exists = False

    if hasattr(hdlr_mod, "operation"):
        op = hdlr_mod.operation(session, meta, **options)
    else:
        op = Operation.REGISTER_SYNC

    if op == Operation.NO_OP:
        if not exists:
            call(hdlr_mod, "on_data_obj_create", no_op, logger, hdlr_mod, logger, session, meta, **options)
        else:
            call(hdlr_mod, "on_data_obj_modify", no_op, logger, hdlr_mod, logger, session, meta, **options)
    else:
        createRepl = False
        if exists and op == Operation.REGISTER_AS_REPLICA_SYNC:
            if hasattr(hdlr_mod, "to_resource"):
                resc_name = hdlr_mod.to_resource(session, meta, **options)
            else:
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
                    raise Exception("there is at least one replica under resource but all replicas have wrong paths")
            else:
                createRepl = True

        put = op in [Operation.PUT, Operation.PUT_SYNC, Operation.PUT_APPEND]
        sync = op in [Operation.PUT_SYNC, Operation.PUT_APPEND]

        if not exists:
            meta2 = meta.copy()
            meta2["target"] = dirname(target)
            if 'b64_path_str' not in meta2:
                meta2["path"] = dirname(path)
            create_dirs(hdlr_mod, logger, session, meta2, **options)
            if put:
                call(hdlr_mod, "on_data_obj_create", upload_file, logger, hdlr_mod, logger, session, meta, **options)
            else:
                call(hdlr_mod, "on_data_obj_create", register_file, logger, hdlr_mod, logger, session, meta, **options)
        elif createRepl:
            options["regRepl"] = ""

            call(hdlr_mod, "on_data_obj_create", register_file, logger, hdlr_mod, logger, session, meta, **options)
        elif content:
            if put:
                if sync:
                    call(hdlr_mod, "on_data_obj_modify", sync_file, logger, hdlr_mod, logger, session, meta, **options)
            else:
                call(hdlr_mod, "on_data_obj_modify", update_metadata, logger, hdlr_mod, logger, session, meta, **options)
        else:
            call(hdlr_mod, "on_data_obj_modify", sync_file_meta, logger, hdlr_mod, logger, session, meta, **options)

    start_timer()

def sync_metadata_from_file(meta, logger, **options):
    sync_data_from_file(meta, logger, False, **options)

def sync_dir_meta(hdlr_mod, logger, session, meta, **options):
    pass

def sync_data_from_dir(meta, logger, content, **options):
    target = meta["target"]
    path = meta["path"]
    hdlr_mod = get_hdlr_mod(meta)
    session = irods_session(hdlr_mod, meta, logger, **options)
    exists = session.collections.exists(target)

    if hasattr(hdlr_mod, "operation"):
        op = hdlr_mod.operation(session, meta, **options)
    else:
        op = Operation.REGISTER_SYNC

    if op == Operation.NO_OP:
        if not exists:
            call(hdlr_mod, "on_coll_create", no_op, logger, hdlr_mod, logger, session, meta, **options)
        else:
            call(hdlr_mod, "on_coll_modify", no_op, logger, hdlr_mod, logger, session, meta, **options)
    else:
        if not exists:
            create_dirs(hdlr_mod, logger, session, meta, **options)
        else:
            call(hdlr_mod, "on_coll_modify", sync_dir_meta, logger, hdlr_mod, logger, session, meta, **options)
    start_timer()

def sync_metadata_from_dir(meta, logger, **options):
    sync_data_from_dir(meta, logger, False, **options)
