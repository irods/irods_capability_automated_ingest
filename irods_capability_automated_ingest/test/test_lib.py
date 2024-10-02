import os

from irods.session import iRODSSession

from irods_capability_automated_ingest.redis_utils import get_redis

# This is a global in order to take advantage of "caching" the Redis configuration.
# Modify get_redis_config if changes are needed.
redis_config = {}


# TODO(#286): Derive from the environment?
def get_redis_config(host="redis", port=6379, db=0):
    global redis_config
    if redis_config:
        return redis_config
    redis_config = {"redis": {"host": host, "port": port, "db": db}}
    return redis_config


def clear_redis():
    get_redis(get_redis_config()).flushdb()


def get_test_irods_client_environment_dict():
    # TODO(#286): Derive from the environment?
    return {
        "host": os.environ.get("IRODS_HOST"),
        "port": os.environ.get("IRODS_PORT"),
        "user": os.environ.get("IRODS_USER_NAME"),
        "zone": os.environ.get("IRODS_ZONE_NAME"),
        "password": os.environ.get("IRODS_PASSWORD"),
    }


def irmtrash():
    # TODO(irods/python-irodsclient#182): Needs irmtrash endpoint
    with iRODSSession(**get_test_irods_client_environment_dict()) as session:
        rods_trash_path = "/".join(
            ["", session.zone, "trash", "home", session.username]
        )
        rods_trash_coll = session.collections.get(rods_trash_path)
        for coll in rods_trash_coll.subcollections:
            delete_collection_if_exists(coll.path, recurse=True, force=True)


def delete_collection_if_exists(coll, recurse=True, force=False):
    with iRODSSession(**get_test_irods_client_environment_dict()) as session:
        if session.collections.exists(coll):
            session.collections.remove(coll, recurse=recurse, force=force)
