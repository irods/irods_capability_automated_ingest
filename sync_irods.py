import os
from os.path import dirname
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


def register_file(session, target, path):
    logger.info("registering object " + target)
    session.data_objects.register(path, target)

def upload_file(session, target, path):
    logger.info("uploading object " + target)
    session.data_objects.put(path, target)

def sync_file(session, target, path):
    upload_file(session, target, path)
    
def update_metadata(session, target, path):
    logger.info("updating object: unregistering " + target)
    session.data_objects.get(target).unregister()
    logger.info("updating object: registering " + target)
    session.data_objects.register(path, target)

def sync_data_from_file(target, path):
    try:
        env_file = os.environ['IRODS_ENVIRONMENT_FILE']
    except KeyError:
        env_file = os.path.expanduser('~/.irods/irods_environment.json')
        
    with iRODSSession(irods_env_file=env_file) as session:
        remote_host = session.host not in ('localhost', socket.gethostname())
        if session.data_objects.exists(target):
            if remote_host:
                sync_file(session, target, path)
            else:
                update_metadata(session, target, path)
        elif session.collections.exists(target):
            raise Exception("sync: cannot syncing file " + path + " to collection " + target)
        else:
            create_dirs(session, dirname(target))
            if remote_host:
                upload_file(session, target, path)
            else:
                register_file(session, target, path)

                
        
