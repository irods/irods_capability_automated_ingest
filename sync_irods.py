import os
from os.path import dirname, getsize, getmtime
from irods.session import iRODSSession
import logging
import sys
import socket

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)

def create_dirs(session, target):
    if not session.collections.exists(target):
        if target == "/":
            raise Exception("create_dirs: Cannot create root")
        create_dirs(session, dirname(target))
        logger.info("creating collection " + target)
        session.collections.create(target)


def register_file(session, target, path, **options):
    logger.info("registering object " + target + ", options = " + str(options))
    session.data_objects.register(path, target, **options)

def upload_file(session, target, path, **options):
    logger.info("uploading object " + target + ", options = " + str(options))
    session.data_objects.put(path, target)

def sync_file(session, target, path, **options):
    logger.info("syncing object " + target + ", options = " + str(options))
    upload_file(session, target, path, **options)
    
def update_metadata(session, target, path, **options):
    logger.info("updating object: " + target + ", options = " + str(options))
    size = getsize(path)
    mtime = int(getmtime(path))
    session.data_objects.modDataObjMeta(target, {"dataSize":size, "dataModify":mtime})

def sync_data_from_file(target, path, hdlr, **options):
    if hdlr is not None:
        hdlr_mod = __import__(hdlr)
    else:
        hdlr_mod = None

    env_file = os.environ.get('IRODS_ENVIRONMENT_FILE')
    if env_file is None:
        env_file = os.path.expanduser('~/.irods/irods_environment.json')

    if hasattr(hdlr_mod, "as_user"):
        client_zone, client_user = hdlr_mod.as_user(target, path, **options)
        options["client_zone"] = client_zone
        options["client_user"] = client_user
        sess_ctx = iRODSSession(irods_env_file=env_file, client_user = client_user, client_zone = client_zone)
    else:
        sess_ctx = iRODSSession(irods_env_file=env_file)
        
    with sess_ctx as session:
        remote_host = session.host not in ('localhost', socket.gethostname())
        
        if hasattr(hdlr_mod, "to_resource_hier"):
            options["resc_hier"] = hdlr_mod.to_resource_hier(session, target, path, **options)
            
        if hasattr(hdlr_mod, "to_resource"):
            options["destRescName"] = hdlr_mod.to_resource(session, target, path, **options)
            
        if session.data_objects.exists(target):
            create = False
            if remote_host:
                sync_file(session, target, path, **options)
            else:
                update_metadata(session, target, path, **options)
        elif session.collections.exists(target):
            raise Exception("sync: cannot syncing file " + path + " to collection " + target)
        else:
            create = True
            create_dirs(session, dirname(target))
            if remote_host:
                upload_file(session, target, path, **options)
            else:
                register_file(session, target, path, **options)

        if create and hasattr(hdlr_mod, "on_create"):
            logger.info("calling create event handler: " + target)
            hdlr_mod.on_create(session, target, path, **options)
        elif hasattr(hdlr_mod, "on_modify"):
            logger.info("calling modify event handler: " + target)
            hdlr_mod.on_modify(session, target, path, **options)

                
        
