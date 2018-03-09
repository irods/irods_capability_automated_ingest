import os
from os.path import dirname, getsize, getmtime, basename
from irods.session import iRODSSession
from irods.models import Resource, DataObject, Collection
import logging
import sys
import importlib
from sync_utils import size

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)


def call(hdlr_mod, hdlr, func, *args, **options):
    if hasattr(hdlr_mod, hdlr):
        logger.debug("calling " + hdlr + " event handler: args = " + str(args) + ", options = " + str(options))
        getattr(hdlr_mod, hdlr)(func, *args, **options)
    else:
        func(*args, **options)

def child_of(session, child_resc_name, resc_name):
    if child_resc_name == resc_name:
        return True
    else:
        while True:
            child_resc = session.resources.get(child_resc_name)
            parent_resc_id = child_resc.parent
            if parent_resc_id == None:
                break

            for row in session.query(Resource.name).filter(Resource.id == parent_resc_id):
                parent_resc_name = row[Resource.name]
            if parent_resc_name == resc_name:
                return True
            child_resc_name = parent_resc_name
        return False
    
def create_dirs(hdlr_mod, session, target, path, **options):
    if not session.collections.exists(target):
        if target == "/":
            raise Exception("create_dirs: Cannot create root")
        create_dirs(hdlr_mod, session, dirname(target), dirname(path), **options)

        def ccfunc(hdlr_mod, session, target, path, **options):
            logger.info("creating collection " + target)
            session.collections.create(target)

        call(hdlr_mod, "on_coll_create", ccfunc, hdlr_mod, session, target, path, **options)

def register_file(hdlr_mod, session, target, path, **options):
    if hasattr(hdlr_mod, "to_resource"):
        options["destRescName"] = hdlr_mod.to_resource(session, target, path, **options)

    logger.info("registering object " + target + ", options = " + str(options))
    session.data_objects.register(path, target, **options)
    size = getsize(path)

    data_obj_info = {"objPath": target}
    if "destRescName" in options:
        for row in session.query(DataObject.replica_number).filter(DataObject.name == basename(target), Collection.name == dirname(target), DataObject.resource_name == options["destRescName"]):
            data_obj_info["replNum"] = int(row[DataObject.replica_number])

    session.data_objects.modDataObjMeta(data_obj_info, {"dataSize":size}, **options)

def upload_file(hdlr_mod, session, target, path, **options):
    if hasattr(hdlr_mod, "to_root_resource"):
        options["destRescName"] = hdlr_mod.to_root_resource(session, target, path, **options)

    logger.info("uploading object " + target + ", options = " + str(options))
    session.data_objects.put(path, target, **options)

def sync_file(hdlr_mod, session, target, path, **options):
    logger.info("syncing object " + target + ", options = " + str(options))
    
    if hasattr(hdlr_mod, "to_root_resource"):
        options["destRescName"] = hdlr_mod.to_root_resource(session, target, path, **options)

    append = False
    if hasattr(hdlr_mod, "append"):
        append = hdlr_mod.append(session, target, path, **options)

    if append:
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
        
    else:
        logger.info("uploading object " + target + ", options = " + str(options))
        session.data_objects.put(path, target, **options)
    
def update_metadata(hdlr_mod, session, target, path, **options):
    size = getsize(path)
    mtime = int(getmtime(path))
    logger.info("updating object: " + target + ", options = " + str(options))

    data_obj_info = {"objPath": target}
    if hasattr(hdlr_mod, "to_resource_hier"):
        data_obj_info["rescHier"] = hdlr_mod.to_resource_hier(session, target, path, **options)

    if hasattr(hdlr_mod, "to_resource"):
        resc_name = hdlr_mod.to_resource(session, target, path, **options)
        # data_obj_info["rescName"] = resc_name
    else:
        resc_name = None

    outdated_repl_nums = []
    found = False
    if resc_name is None:
        found = True
    else:
        for row in session.query(Resource.name, DataObject.path, DataObject.replica_number).filter(DataObject.name == basename(target), Collection.name == dirname(target)):
            if row[DataObject.path] == path:
                if child_of(session, row[Resource.name], resc_name):
                    found = True
                    repl_num = row[DataObject.replica_number]
                    data_obj_info["replNum"] = repl_num
                    continue
            outdated_repl_nums.append(row[DataObject.replica_number])

    if not found:
        logger.error("updating object: wrong resource or path, target = " + target + ", path = " + path + ", options = " + str(options))
        raise Exception("wrong resource or path")

    session.data_objects.modDataObjMeta(data_obj_info, {"dataSize":size, "dataModify":mtime, "replStatus":1}, **options)

    outdated_data_obj_info = {"objPath": target}

    for outdated_repl_num in outdated_repl_nums:
        outdated_data_obj_info["replNum"] = outdated_repl_num
        session.data_objects.modDataObjMeta(outdated_data_obj_info, {"replStatus":0})

def sync_file_meta(hdlr_mod, session, target, path, **options):
    pass
    
def sync_data_from_file(target, path, hdlr, content, **options):
    if hdlr is not None:
        hdlr_mod0 = importlib.import_module(hdlr)
        hdlr_mod = getattr(hdlr_mod0, "event_handler", None)
    else:
        hdlr_mod = None

    env_file = os.environ.get('IRODS_ENVIRONMENT_FILE')
    if env_file is None:
        env_file = os.path.expanduser('~/.irods/irods_environment.json')

    if hasattr(hdlr_mod, "as_user"):
        client_zone, client_user = hdlr_mod.as_user(target, path, **options)
        sess_ctx = iRODSSession(irods_env_file=env_file, client_user = client_user, client_zone = client_zone)
    else:
        sess_ctx = iRODSSession(irods_env_file=env_file)

        
    with sess_ctx as session:    

        if session.data_objects.exists(target):
            exists = True
        elif session.collections.exists(target):
            raise Exception("sync: cannot syncing file " + path + " to collection " + target)
        else:
            exists = False

        put = False
        if hasattr(hdlr_mod, "put"):
            put = hdlr_mod.put(session, target, path, **options)
                    
        replica = False
        if hasattr(hdlr_mod, "as_replica"):
            replica = hdlr_mod.as_replica(session, target, path, **options)

        createRepl = False
        if exists and replica:
            if hasattr(hdlr_mod, "to_resource"):
                resc_name = hdlr_mod.to_resource(session, target, path, **options)
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

        if not exists:
            create_dirs(hdlr_mod, session, dirname(target), dirname(path), **options)

        if not exists:
            if put:
                call(hdlr_mod, "on_data_obj_create", upload_file, hdlr_mod, session, target, path, **options)
            else:
                call(hdlr_mod, "on_data_obj_create", register_file, hdlr_mod, session, target, path, **options)
        elif createRepl:
            if put:
                call(hdlr_mod, "on_data_obj_create", upload_file, hdlr_mod, session, target, path, **options)
            else:
                options["regRepl"] = ""

                call(hdlr_mod, "on_data_obj_create", register_file, hdlr_mod, session, target, path, **options)
        elif content:
            if put:
                sync = True
                if hasattr(hdlr_mod, "sync"):
                    sync = hdlr_mod.sync(session, target, path, **options)

                if sync:
                    call(hdlr_mod, "on_data_obj_modify", sync_file, hdlr_mod, session, target, path, **options)
            else:
                call(hdlr_mod, "on_data_obj_modify", update_metadata, hdlr_mod, session, target, path, **options)
        else:
            call(hdlr_mod, "on_data_obj_modify", sync_file_meta, hdlr_mod, session, target, path, **options)

def sync_metadata_from_file(target, path, hdlr, put, **options):
    sync_data_from_file(target, path, hdlr, put, False, **options)
