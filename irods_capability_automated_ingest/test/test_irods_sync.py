import base64
import glob
import os
import re
import os.path
import stat
import subprocess
import sys
import time
import traceback
import unittest
from signal import SIGINT
from os import makedirs, listdir, remove
from shutil import rmtree
from os.path import join, realpath, getmtime, getsize, dirname, basename, relpath, isfile
from irods.session import iRODSSession
from irods.models import Collection, DataObject
from tempfile import NamedTemporaryFile, mkdtemp
from datetime import datetime
from irods_capability_automated_ingest.sync_utils import size, app
from irods_capability_automated_ingest.sync_utils import get_redis as sync_utils_get_redis
from irods_capability_automated_ingest.sync_job import sync_job
from irods.data_object import irods_dirname, irods_basename
import irods_capability_automated_ingest.examples
import irods.keywords as kw

LOG_FILE = "/tmp/a"

ZONENAME = "tempZone"
RODSADMIN = "rods"

IRODS_SYNC_PY = "irods_capability_automated_ingest.irods_sync"

PATH_TO_SOURCE_DIR = join('/', 'tmp', 'testdir', 'a')
A_REMOTE = "a_remote"
PATH_TO_COLLECTION = "/" + ZONENAME + "/home/" + RODSADMIN + "/" + A_REMOTE

NFILES = 10
NWORKERS = 10
TIMEOUT = 60
DEFAULT_JOB_NAME = 'test_irods_sync'

DEFAULT_RESC = "demoResc"
DEFAULT_RESC_VAULT_PATH = "/var/lib/irods/Vault"

REGISTER_RESC = "regiResc"
REGISTER_RESC_PATH = "/var/lib/irods/Vault2"

REGISTER_RESC2_ROOT = "regiResc2Root"

REGISTER_RESC2 = "regiResc2"

REGISTER_RESC2A = "regiResc2a"
REGISTER_RESC_PATH2A = "/var/lib/irods/Vault2a"
REGISTER_RESC2B = "regiResc2b"
REGISTER_RESC_PATH2B = "/var/lib/irods/Vault2b"

PUT_RESC = "putResc"
PUT_RESC_PATH = "/var/lib/irods/Vault3"

HIERARCHY1 = {
    REGISTER_RESC : {
        "type" : "unixfilesystem",
        "kwargs" : {
            "host":  os.environ.get("IRODS_HOST"),
            "path": REGISTER_RESC_PATH
        }
    },
    PUT_RESC : {
        "type" : "unixfilesystem",
        "kwargs" : {
            "host": os.environ.get("IRODS_HOST"),
            "path": PUT_RESC_PATH
        }
    },
    REGISTER_RESC2_ROOT : {
        "type": "random",
        "children" : {
            REGISTER_RESC2: {
                "type": "random",
                "children": {
                    REGISTER_RESC2A: {
                        "type": "unixfilesystem",
                        "kwargs": {
                            "host": os.environ.get("IRODS_HOST"),
                            "path": REGISTER_RESC_PATH2A
                        }
                    },
                    REGISTER_RESC2B: {
                        "type": "unixfilesystem",
                        "kwargs": {
                            "host": os.environ.get("IRODS_HOST"),
                            "path": REGISTER_RESC_PATH2B
                        }
                    },
                }
            }
        }
    }
}

def get_irods_environment_file():
    env_file = os.environ.get('IRODS_ENVIRONMENT_FILE')
    if env_file is None:
        env_file = os.path.expanduser('~/.irods/irods_environment.json')
        if not os.exists(env_file):
            env_file = None
    return env_file

def get_kwargs():
    kwargs = {}
    #env_file = get_irods_environment_file()
    #if env_file:
        #kwargs['irods_env_file'] = env_file
        #return kwargs

    env_irods_host = os.environ.get("IRODS_HOST")
    env_irods_port = os.environ.get("IRODS_PORT")
    env_irods_user_name = os.environ.get("IRODS_USER_NAME")
    env_irods_zone_name = os.environ.get("IRODS_ZONE_NAME")
    env_irods_password = os.environ.get("IRODS_PASSWORD")

    kwargs["host"] = env_irods_host
    kwargs["port"] = env_irods_port
    kwargs["user"] = env_irods_user_name
    kwargs["zone"] = env_irods_zone_name
    kwargs["password"] = env_irods_password

    return kwargs

def get_redis(host='redis', port=6379, db=0):
    redis_config = {}
    redis_config['host'] = host
    redis_config['port'] = port
    redis_config['db'] = db
    config = {}
    config['redis'] = redis_config
    return sync_utils_get_redis(config)

def clear_redis():
    get_redis().flushdb()


def start_workers(n, args=[]):
    workers = subprocess.Popen(["celery", "-A", "irods_capability_automated_ingest.sync_task", "worker", "-c", str(n), "-l", "info", "-Q", "restart,path,file"] + args)
    return workers


def wait_for(workers, job_name = DEFAULT_JOB_NAME):
    r = get_redis()
    t0 = time.time()
    while TIMEOUT is None or time.time() - t0 < TIMEOUT:
        restart = r.llen("restart")
        i = app.control.inspect()
        act = i.active()
        if act is None:
            active = 0
        else:
            active = sum(map(len, act.values()))
        d = sync_job(job_name, r).done()
        if restart != 0 or active != 0 or not d:
            time.sleep(1)
        else:
            break

    workers.send_signal(SIGINT)
    workers.wait()


def create_files(nfiles):
    create_files2(0, nfiles)


def create_files2(depth, nfiles):
    a = join(PATH_TO_SOURCE_DIR, *list(map(lambda i: "a" + str(i), range(depth))))
    makedirs(a)
    for i in range(nfiles):
        with open(join(a, str(i)), "w") as f:
            f.write("i" * i)


def recreate_files(nfiles, depth=0):
    a = join(PATH_TO_SOURCE_DIR, *list(map(lambda i: "a" + str(i), range(depth))))
    for i in range(nfiles):
        with open(join(a, str(i)), "w") as f:
            f.write("i" * (i * 2 + 1))


def ctime_files(nfiles=NFILES):
    for i in range(nfiles):
        os.chmod(join(PATH_TO_SOURCE_DIR, str(i)), stat.S_IRUSR )
        os.chmod(join(PATH_TO_SOURCE_DIR, str(i)), stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)


def delete_files():
    rmtree(PATH_TO_SOURCE_DIR)
    if isfile(LOG_FILE):
        remove(LOG_FILE)


def read_file(path):
    with open(path) as f:
        return f.read()


def hierarchy_string_for_leaf (session, logical_path, leafName):
    ptn = re.compile(";" + leafName + "$")
    equals_or_is_leaf_of = lambda leaf,hierstr : leaf == hierstr or ptn.search(hierstr)
    q = session.query(DataObject).filter(DataObject.name == irods_basename(logical_path),
                                         Collection.name == irods_dirname(logical_path))
    hierstr = [ r[DataObject.resc_hier] for r in q if equals_or_is_leaf_of(leafName,r[DataObject.resc_hier]) ]
    return hierstr[0] if hierstr else ''


def read_data_object(session, path, resc_name = DEFAULT_RESC):
    with NamedTemporaryFile() as tf:
        resc_hier = hierarchy_string_for_leaf(session, path, resc_name)
        options = {kw.RESC_HIER_STR_KW:resc_hier} if resc_hier \
             else {kw.RESC_NAME_KW:resc_name}
        session.data_objects.get(path, tf.name, forceFlag="", **options)
        return read_file(tf.name)


def create_resource(session, resc_name, resc_dict, root = None):
    if "kwargs" in resc_dict:
        session.resources.create(resc_name, resc_dict["type"], **resc_dict["kwargs"])
    else:
        session.resources.create(resc_name, resc_dict["type"])

    if root is not None:
        session.resources.add_child(root, resc_name)

    if resc_dict.get("children") is not None:
        create_resources(session, resc_dict["children"], resc_name)


def create_resources(session, hierarchy, root = None):
    for resc_name, resc_dict in hierarchy.items():
        create_resource(session, resc_name, resc_dict, root)


def delete_resource(session, resc_name, resc_dict, root = None):
    if resc_dict.get("children") is not None:
        delete_resources(session, resc_dict["children"], resc_name)

    if root is not None:
        session.resources.remove_child(root, resc_name)

    session.resources.remove(resc_name)


def delete_resources(session, hierarchy, root = None):
    for resc_name, resc_dict in hierarchy.items():
        delete_resource(session, resc_name, resc_dict, root)


def irmtrash():
    # TODO: irods/python-irodsclient#182 Needs irmtrash endpoint
    with iRODSSession(**get_kwargs()) as session:
        rods_trash_path = join('/', session.zone, 'trash', 'home', session.username)
        rods_trash_coll = session.collections.get(rods_trash_path)
        for coll in rods_trash_coll.subcollections:
            delete_collection_if_exists(coll.path, recurse=True, force=True)


def delete_collection(coll, recurse=True, force=False):
    with iRODSSession(**get_kwargs()) as session:
        session.collections.remove(coll)


def delete_collection_if_exists(coll, recurse=True, force=False):
    with iRODSSession(**get_kwargs()) as session:
        if(session.collections.exists(coll)):
           session.collections.remove(coll, recurse=recurse, force=force)


def modify_time(session, path):
    for row in session.query(DataObject.modify_time).filter(Collection.name == dirname(path), DataObject.name == basename(path)):
        return row[DataObject.modify_time]


def event_handler_path(eh_name):
    return os.path.join(sys.modules["irods_capability_automated_ingest.examples"].__path__[0], eh_name + '.py')

class automated_ingest_test_context(object):
    def setUp(self):
        os.environ["CELERY_BROKER_URL"] = "redis://redis:6379/0"

        irmtrash()
        clear_redis()
        delete_collection_if_exists(PATH_TO_COLLECTION)
        create_files(NFILES)
        with iRODSSession(**get_kwargs()) as session:
            create_resources(session, HIERARCHY1)

        self.logfile = NamedTemporaryFile()

    def tearDown(self):
        delete_files()
        clear_redis()
        delete_collection_if_exists(PATH_TO_COLLECTION)
        irmtrash()
        with iRODSSession(**get_kwargs()) as session:
            delete_resources(session, HIERARCHY1)

    # utilities
    def do_register(self, eh_name, job_name = DEFAULT_JOB_NAME, resc_name = [DEFAULT_RESC]):
        eh = event_handler_path(eh_name)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        self.do_register2(job_name, resc_names=resc_name)

    def do_register2(self, job_name = DEFAULT_JOB_NAME, resc_names=[DEFAULT_RESC]):
        workers = start_workers(1)
        wait_for(workers, job_name)
        self.do_assert_register(resc_names)

    def do_assert_register(self, resc_names):
        with iRODSSession(**get_kwargs()) as session:
            self.assertTrue(session.collections.exists(PATH_TO_COLLECTION))
            for i in listdir(PATH_TO_SOURCE_DIR):
                path = join(PATH_TO_SOURCE_DIR, i)
                rpath = PATH_TO_COLLECTION + "/" + i
                self.assertTrue(session.data_objects.exists(rpath))
                a1 = read_file(path)

                a2 = read_data_object(session, rpath)
                self.assertEqual(a1, a2)

                obj = session.data_objects.get(rpath)
                self.assertEqual(obj.replicas[0].path, realpath(path))
                self.assertIn(obj.replicas[0].resource_name, resc_names)
                s1 = getsize(path)
                mtime1 = int(getmtime(path))
                s2 = size(session, rpath)
                mtime2 = modify_time(session, rpath)
                self.assertEqual(s1, s2)
                self.assertEqual(datetime.utcfromtimestamp(mtime1), mtime2)

    def do_put(self, eh_name, job_name = DEFAULT_JOB_NAME, resc_names = [DEFAULT_RESC], resc_roots = [DEFAULT_RESC_VAULT_PATH]):
        eh = event_handler_path(eh_name)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers, job_name)

        self.do_assert_put(resc_names, resc_roots)

    def do_assert_put(self, resc_names, resc_roots):
        with iRODSSession(**get_kwargs()) as session:
            self.assertTrue(session.collections.exists(PATH_TO_COLLECTION))
            for i in listdir(PATH_TO_SOURCE_DIR):
                path = join(PATH_TO_SOURCE_DIR, i)
                rpath = PATH_TO_COLLECTION + "/" + i
                vaultpaths = map(lambda resc_root : resc_root + "/home/" + RODSADMIN + "/" + A_REMOTE + "/" + i, resc_roots)
                self.assertTrue(session.data_objects.exists(rpath))
                a1 = read_file(path)

                a2 = read_data_object(session, rpath)
                self.assertEqual(a1, a2)

                obj = session.data_objects.get(rpath)
                self.assertIn(obj.replicas[0].path, vaultpaths)
                self.assertIn(obj.replicas[0].resource_name, resc_names)

    def do_assert_failed_queue(self, error_message=None, count=NFILES, job_name = DEFAULT_JOB_NAME):
        self.assertEqual(sync_job(job_name, get_redis()).failures_handle().get_value(), count)

    def do_assert_retry_queue(self, error_message=None, count=NFILES, job_name = DEFAULT_JOB_NAME):
        self.assertEqual(sync_job(job_name, get_redis()).retries_handle().get_value(), count)


class Test_event_handlers(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_event_handlers, self).setUp()

    def tearDown(self):
        super(Test_event_handlers, self).tearDown()

    # no event handler
    def do_no_event_handler(self, job_name = DEFAULT_JOB_NAME):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--log_level", "INFO"])
        proc.wait()
        self.do_register2(job_name)

    def test_no_event_handler(self):
        job_name = 'test_no_event_handler'
        self.do_no_event_handler(job_name = job_name)
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

    # no op
    def do_no_op(self, eh_name, job_name = DEFAULT_JOB_NAME):
        eh = event_handler_path(eh_name)

        recreate_files(NFILES)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers, job_name)

    def test_no_op(self):
        job_name = 'test_no_op.do_no_op'
        self.do_no_op("no_op", job_name = job_name)
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)


    # append_json
    def do_append_json(self, eh_name, job_name = DEFAULT_JOB_NAME):
        eh = event_handler_path(eh_name)

        recreate_files(NFILES)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--append_json", "\"append_json\"", "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers, job_name)

        self.do_assert_failed_queue(count=None, job_name=job_name)

    def test_append_json(self):
        job_name = 'test_append_json'
        self.do_append_json(
            "append_json",
            job_name = job_name)


    # create dir
    def do_register_dir_par(self, eh_name, job_name = DEFAULT_JOB_NAME, resc_names=[DEFAULT_RESC]):
        eh = event_handler_path(eh_name)

        create_files2(10, NFILES)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(NWORKERS)
        wait_for(workers, job_name)

        with iRODSSession(**get_kwargs()) as session:
            self.assertTrue(session.collections.exists(PATH_TO_COLLECTION))
            for i in glob.glob(PATH_TO_SOURCE_DIR+"/**/*", recursive=True):
                if isfile(i):
                    path = i
                    rpath = PATH_TO_COLLECTION + "/" + relpath(i, PATH_TO_SOURCE_DIR)
                    self.assertTrue(session.data_objects.exists(rpath))
                    a1 = read_file(path)

                    a2 = read_data_object(session, rpath)
                    self.assertEqual(a1, a2)

                    obj = session.data_objects.get(rpath)
                    self.assertEqual(obj.replicas[0].path, realpath(path))
                    self.assertIn(obj.replicas[0].resource_name, resc_names)
                    s1 = getsize(path)
                    mtime1 = int(getmtime(path))
                    s2 = size(session, rpath)
                    mtime2 = modify_time(session, rpath)
                    self.assertEqual(s1, s2)
                    self.assertEqual(datetime.utcfromtimestamp(mtime1), mtime2)

    def test_create_dir(self):
        job_name = 'test_create_dir'
        self.do_register_dir_par(
            "register",
            job_name = job_name)
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

    # timeout
    def do_timeout(self, eh_name, job_name = DEFAULT_JOB_NAME):
        eh = event_handler_path(eh_name)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers, job_name)

        self.do_assert_failed_queue(count=10, job_name=job_name)

    def test_timeout(self):
        job_name = 'test_timeout.do_timeout'
        self.do_timeout(
            "timeout",
            job_name = job_name)

class Test_retry(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_retry, self).setUp()

    def tearDown(self):
        super(Test_retry, self).tearDown()

    def do_no_retry(self, eh_name, job_name = DEFAULT_JOB_NAME, resc_name = [DEFAULT_RESC]):
        eh = event_handler_path(eh_name)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(1)
        wait_for(workers, job_name)

        self.do_assert_failed_queue("no failures", job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

    def do_retry(self, eh_name, job_name = DEFAULT_JOB_NAME, resc_name = [DEFAULT_RESC]):
        eh = event_handler_path(eh_name)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(1)
        wait_for(workers, job_name)

        self.do_assert_failed_queue("no failures", count=None, job_name=job_name)
        self.do_assert_retry_queue(job_name=job_name)

    def test_no_retry(self):
        job_name = 'test_retry.do_no_retry'
        self.do_no_retry(
            "no_retry",
            job_name = job_name)

    def test_retry(self):
        job_name = 'test_retry.do_retry'
        self.do_retry(
            "retry",
            job_name = job_name)


class Test_pre_and_post_job(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_pre_and_post_job, self).setUp()

    def tearDown(self):
        super(Test_pre_and_post_job, self).tearDown()

    def do_pre_job(self, eh_name, job_name = DEFAULT_JOB_NAME):
        eh = event_handler_path(eh_name)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers, job_name)
        with open(LOG_FILE,"r") as f:
            lines = f.readlines()
            self.assertEqual(lines, ["pre_job"])

    def do_post_job(self, eh_name, job_name = DEFAULT_JOB_NAME):
        eh = event_handler_path(eh_name)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers, job_name)
        with open(LOG_FILE,"r") as f:
            lines = f.readlines()
            self.assertEqual(lines, ["post_job"])

    def test_pre_job(self):
        job_name = 'test_pre_job.do_pre_job'
        self.do_pre_job(
            "pre_job",
            job_name = job_name)

    def test_post_job(self):
        job_name = 'test_post_job.do_post_job'
        self.do_post_job(
            "post_job",
            job_name = job_name)

class Test_no_sync(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_no_sync, self).setUp()

    def tearDown(self):
        super(Test_no_sync, self).tearDown()

    def do_no_sync(self, eh_name, job_name = DEFAULT_JOB_NAME):
        eh = event_handler_path(eh_name)

        recreate_files(NFILES)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers, job_name)

        with iRODSSession(**get_kwargs()) as session:
            for i in listdir(PATH_TO_SOURCE_DIR):
                path = join(PATH_TO_SOURCE_DIR, i)
                rpath = PATH_TO_COLLECTION + "/" + i
                a1 = read_file(path)

                a2 = read_data_object(session, rpath)
                self.assertNotEqual(a1, a2)

    def test_no_sync(self):
        put_job = 'test_no_sync.put'
        self.do_put(
            "put",
            job_name = put_job)
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        no_sync_job = 'test_no_sync.no_sync'
        self.do_no_sync(
            "put",
            job_name = no_sync_job)
        self.do_assert_failed_queue(count=None, job_name=no_sync_job)
        self.do_assert_retry_queue(count=None, job_name=no_sync_job)

    def test_no_sync_root_with_resc_name(self):
        put_job = 'test_no_sync_root_with_resc_name.put'
        self.do_put(
            "put_root_with_resc_name",
            job_name = put_job,
            resc_names = [REGISTER_RESC2A, REGISTER_RESC2B],
            resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        no_sync_job = 'test_no_sync_root_with_resc_name.no_sync'
        self.do_no_sync(
            "put_root_with_resc_name",
            job_name = no_sync_job)
        self.do_assert_failed_queue(count=None, job_name=no_sync_job)
        self.do_assert_retry_queue(count=None, job_name=no_sync_job)

    def test_no_sync_with_resc_name(self):
        # Identical to test_put_with_resc_name
        pass

    def test_no_sync_non_leaf_non_root_with_resc_name(self):
        # Identical to test_put_non_leaf_non_root_with_resc_name
        pass


class Test_sync(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_sync, self).setUp()

    def tearDown(self):
        super(Test_sync, self).tearDown()

    def test_sync(self):
        put_job = 'test_sync.put'
        self.do_put(
            "sync",
            job_name = put_job)
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        recreate_job = 'test_sync.recreate'
        recreate_files(NFILES)
        self.do_put(
            "sync",
            job_name = recreate_job)
        self.do_assert_failed_queue(count=None, job_name=recreate_job)
        self.do_assert_retry_queue(count=None, job_name=recreate_job)

    def test_sync_root_with_resc_name(self):
        put_job = 'test_sync_root_with_resc_name.put'
        self.do_put(
            "sync_root_with_resc_name",
            job_name = put_job,
            resc_names = [REGISTER_RESC2A, REGISTER_RESC2B],
            resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        recreate_job = 'test_sync_root_with_resc_name.recreate'
        recreate_files(NFILES)
        self.do_put(
            "sync_root_with_resc_name",
            job_name = recreate_job,
            resc_names = [REGISTER_RESC2A, REGISTER_RESC2B],
            resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None, job_name=recreate_job)
        self.do_assert_retry_queue(count=None, job_name=recreate_job)

    def test_sync_with_resc_name(self):
        # Identical to test_put_with_resc_name
        pass

    def test_sync_non_leaf_non_root_with_resc_name(self):
        # Identical to test_put_non_leaf_non_root_with_resc_name
        pass

class Test_update(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_update, self).setUp()

    def tearDown(self):
        super(Test_update, self).tearDown()

    def do_update(self, eh_name, job_name = DEFAULT_JOB_NAME, resc_name = [DEFAULT_RESC]):
        recreate_files(NFILES)
        self.do_register(eh_name, job_name, resc_name = resc_name)
        with iRODSSession(**get_kwargs()) as session:
            for i in listdir(PATH_TO_SOURCE_DIR):
                path = join(PATH_TO_SOURCE_DIR, i)
                rpath = PATH_TO_COLLECTION + "/" + i
                s1 = getsize(path)
                mtime1 = int(getmtime(path))
                s2 = size(session, rpath)
                mtime2 = modify_time(session, rpath)
                self.assertEqual(s1, s2)
                self.assertEqual(datetime.utcfromtimestamp(mtime1), mtime2)

    def test_update(self):
        register_job = 'test_update.register'
        self.do_register(
            "register",
            job_name = register_job)
        self.do_assert_failed_queue(count=None, job_name=register_job)
        self.do_assert_retry_queue(count=None, job_name=register_job)

        update_job = 'test_update.update'
        self.do_update(
            "register",
            job_name = update_job)
        self.do_assert_failed_queue(count=None, job_name=update_job)
        self.do_assert_retry_queue(count=None, job_name=update_job)

    def test_update_with_resc_name(self):
        register_job = 'test_update_with_resc_name.register'
        self.do_register(
            "register_with_resc_name",
            job_name = register_job,
            resc_name = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None, job_name=register_job)
        self.do_assert_retry_queue(count=None, job_name=register_job)

        update_job = 'test_update_with_resc_name.update'
        self.do_update(
            "register_with_resc_name",
            job_name = update_job,
            resc_name = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None, job_name=update_job)
        self.do_assert_retry_queue(count=None, job_name=update_job)

    def test_update_root_with_resc_name(self):
        register_job = 'test_update_root_with_resc_name.register'
        self.do_register(
            "register_root_with_resc_name",
            job_name = register_job,
            resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None, job_name=register_job)
        self.do_assert_retry_queue(count=None, job_name=register_job)

        update_job = 'test_update_root_with_resc_name.update'
        self.do_update(
            "register_root_with_resc_name",
            job_name = update_job,
            resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None, job_name=update_job)
        self.do_assert_retry_queue(count=None, job_name=update_job)

    def test_update_non_leaf_non_root_with_resc_name(self):
        # Identical to test_register_non_leaf_non_root_with_resc_name
        pass

class Test_put(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_put, self).setUp()

    def tearDown(self):
        super(Test_put, self).tearDown()

    def do_put_par(self, eh_name, job_name = DEFAULT_JOB_NAME, resc_names=[DEFAULT_RESC], resc_roots=[DEFAULT_RESC_VAULT_PATH]):
        eh = event_handler_path(eh_name)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(NWORKERS)
        wait_for(workers, job_name)

        self.do_assert_put(resc_names, resc_roots)

    def test_put(self):
        job_name = 'test_put.do_put'
        self.do_put(
            "put",
            job_name = job_name)
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

    def test_put_with_resc_name(self):
        job_name = 'test_put_with_resc_name.put'
        event_handler = event_handler_path('put_with_resc_name')
        proc = subprocess.Popen([
            "python", "-m", IRODS_SYNC_PY, "start",
            PATH_TO_SOURCE_DIR,
            PATH_TO_COLLECTION,
            "--event_handler", event_handler,
            "--job_name", job_name,
            "--log_level", "INFO",
            '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(1)
        wait_for(workers, job_name)
        # This should result in a DIRECT_CHILD_ACCESS
        self.do_assert_failed_queue(job_name=job_name)

    def test_put_root_with_resc_name(self):
        job_name = 'test_put_root_with_resc_name.do_put'
        self.do_put(
            "put_root_with_resc_name",
            job_name = job_name,
            resc_names = [REGISTER_RESC2A, REGISTER_RESC2B],
            resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

    def test_put_non_leaf_non_root_with_resc_name(self):
        job_name = 'test_put_non_leaf_non_root_with_resc_name.put'
        event_handler = event_handler_path('put_non_leaf_non_root_with_resc_name')
        proc = subprocess.Popen([
            "python", "-m", IRODS_SYNC_PY, "start",
            PATH_TO_SOURCE_DIR,
            PATH_TO_COLLECTION,
            "--event_handler", event_handler,
            "--job_name", job_name,
            "--log_level", "INFO",
            '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(1)
        wait_for(workers, job_name)
        # This should result in a DIRECT_CHILD_ACCESS
        self.do_assert_failed_queue(job_name=job_name)

    def test_put_with_multiple_workers(self):
        job_name = 'test_put_with_multiple_workers'
        self.do_put_par(
            "put",
            job_name = job_name)
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

class Test_append(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_append, self).setUp()

    def tearDown(self):
        super(Test_append, self).tearDown()

    def test_append(self):
        put_job = 'test_append.put'
        self.do_put(
            "append",
            job_name = put_job)
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        recreate_job = 'test_append.recreate'
        recreate_files(NFILES)
        self.do_put(
            "append",
            job_name = recreate_job)
        self.do_assert_failed_queue(count=None, job_name=recreate_job)
        self.do_assert_retry_queue(count=None, job_name=recreate_job)

    def test_append_root_with_resc_name(self):
        put_job = 'test_append_root_with_resc_name.put'
        self.do_put(
            'append_root_with_resc_name',
            job_name = put_job,
            resc_names=[REGISTER_RESC2A, REGISTER_RESC2B],
            resc_roots=[REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        recreate_job = 'test_append_root_with_resc_name.recreate'
        recreate_files(NFILES)
        self.do_put(
            "append_root_with_resc_name",
            job_name = recreate_job,
            resc_names=[REGISTER_RESC2A, REGISTER_RESC2B],
            resc_roots=[REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None, job_name=recreate_job)
        self.do_assert_retry_queue(count=None, job_name=recreate_job)

    def test_append_with_resc_name(self):
        # Identical to test_put_with_resc_name
        pass

    def test_append_non_leaf_non_root_with_resc_name(self):
        # Identical to test_put_non_leaf_non_root_with_resc_name
        pass

class Test_pep_callbacks(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_pep_callbacks, self).setUp()

    def tearDown(self):
        super(Test_pep_callbacks, self).tearDown()

    def run_sync_job_with_pep_callbacks(self, source_dir=PATH_TO_SOURCE_DIR, destination_coll=PATH_TO_COLLECTION, job_name = DEFAULT_JOB_NAME):
        eh = event_handler_path('register_with_peps')
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", source_dir, destination_coll, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--log_filename', self.logfile.name, '--files_per_task', '1'])
        proc.wait()

    def assert_pep_messages_in_log(self, log_contents, messages):
        # Ensure that the list of pep messages appears in the log
        for msg in messages:
            count = log_contents.count(msg)
            self.assertEqual(1, count, msg='found {0} occurrences of message:[{1}]'.format(count, msg))

    def test_create_peps(self):
        job_name_base = 'test_create_peps'
        self.run_sync_job_with_pep_callbacks(
            job_name='{}.register_pep'.format(job_name_base))
        self.do_register2(
            job_name='{}.do_register2'.format(job_name_base),
            resc_names = DEFAULT_RESC)

        with open(self.logfile.name, 'r') as f:
            log_contents = f.read()

        files = [os.path.join(PATH_TO_COLLECTION, str(x)) for x in range(NFILES)]
        pep_messages = [['pre_data_obj_create:[' + filepath + ']' for filepath in files],
                        ['post_data_obj_create:[' + filepath + ']' for filepath in files],
                        ['pre_coll_create:[' + PATH_TO_COLLECTION + ']'],
                        ['post_coll_create:[' + PATH_TO_COLLECTION + ']']]
        for messages in pep_messages:
            self.assert_pep_messages_in_log(log_contents, messages)

    def test_modify_peps(self):
        # register directory PATH_TO_SOURCE_DIR
        job_name_base = 'test_modify_peps'
        self.run_sync_job_with_pep_callbacks(
            job_name='{}.register_pep'.format(job_name_base))
        self.do_register2(
            job_name = '{}.do_register2'.format(job_name_base),
            resc_names = DEFAULT_RESC)

        # recreate files and register sync to trigger modify behavior
        rmtree(PATH_TO_SOURCE_DIR)
        create_files(NFILES)
        self.run_sync_job_with_pep_callbacks(
            job_name='{}.modify_pep'.format(job_name_base))
        self.do_register2(
            job_name='{}.register2.modify'.format(job_name_base),
            resc_names = DEFAULT_RESC)

        # Read in log and verify that PEPs fired
        with open(self.logfile.name, 'r') as f:
            log_contents = f.read()
        files = [os.path.join(PATH_TO_COLLECTION, str(x)) for x in range(NFILES)]
        pep_messages = [['pre_data_obj_modify:[' + filepath + ']' for filepath in files],
                        ['post_data_obj_modify:[' + filepath + ']' for filepath in files],
                        ['pre_coll_modify:[' + PATH_TO_COLLECTION + ']'],
                        ['post_coll_modify:[' + PATH_TO_COLLECTION + ']']]
        for messages in pep_messages:
            self.assert_pep_messages_in_log(log_contents, messages)

    def test_empty_coll_create_peps(self):
        empty_dir_tree = join(os.path.dirname(PATH_TO_SOURCE_DIR), 'emptydir')
        subdir_names = ['subdir_a', 'subdir_b', 'subdir_c']
        for subdir in subdir_names:
            os.makedirs(os.path.join(empty_dir_tree, subdir))

        try:
            job_name = 'test_empty_coll_create_peps.register_pep'
            self.run_sync_job_with_pep_callbacks(
                empty_dir_tree,
                job_name = job_name)
            workers = start_workers(1)
            wait_for(workers, job_name = job_name)
            # Assert that the collections were created
            with iRODSSession(**get_kwargs()) as session:
                self.assertTrue(session.collections.exists(PATH_TO_COLLECTION))
                for subdir in subdir_names:
                    self.assertTrue(session.collections.exists(os.path.join(PATH_TO_COLLECTION, subdir)))

            with open(self.logfile.name, 'r') as f:
                log_contents = f.read()

            collections = [os.path.join(PATH_TO_COLLECTION, subdir) for subdir in subdir_names]
            pep_messages = [['pre_coll_create:[' + collection + ']' for collection in collections],
                            ['post_coll_create:[' + collection + ']' for collection in collections]]

            for messages in pep_messages:
                self.assert_pep_messages_in_log(log_contents, messages)

        finally:
            rmtree(empty_dir_tree, ignore_errors=True)

class Test_register_as_replica(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_register_as_replica, self).setUp()

    def tearDown(self):
        super(Test_register_as_replica, self).tearDown()

    def do_register_as_replica_no_assertions(self, eh_name, job_name = DEFAULT_JOB_NAME):
        eh = event_handler_path(eh_name)

        clear_redis()
        recreate_files(NFILES)

        proc = subprocess.Popen([
            "python", "-m", IRODS_SYNC_PY, "start",
            PATH_TO_SOURCE_DIR,
            PATH_TO_COLLECTION,
            "--event_handler", eh,
            "--job_name", job_name,
            "--log_level", "INFO",
            '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers, job_name)

    def do_register_as_replica(self, eh_name, job_name = DEFAULT_JOB_NAME, resc_names = [DEFAULT_RESC]):
        self.do_register_as_replica_no_assertions(eh_name)
        with iRODSSession(**get_kwargs()) as session:
            self.assertTrue(session.collections.exists(PATH_TO_COLLECTION))
            for i in os.listdir(PATH_TO_SOURCE_DIR):
                physical_path_to_new_file = os.path.join(PATH_TO_SOURCE_DIR, i)
                logical_path_to_data_object = os.path.join(PATH_TO_COLLECTION, i)
                self.assertTrue(session.data_objects.exists(logical_path_to_data_object))
                contents_of_new_physical_file = read_file(physical_path_to_new_file)

                obj = session.data_objects.get(logical_path_to_data_object)
                self.assertEqual(len(obj.replicas), 2)
                resc_name_replica_1 = obj.replicas[1].resource_name
                self.assertIn(resc_name_replica_1, resc_names)
                contents_of_replica_1 = read_data_object(session, logical_path_to_data_object, resc_name = resc_name_replica_1)
                self.assertEqual(contents_of_new_physical_file, contents_of_replica_1)
                contents_of_replica_0 = read_data_object(session, logical_path_to_data_object)
                self.assertNotEqual(contents_of_new_physical_file, contents_of_replica_0)
                self.assertNotEqual(size(session, logical_path_to_data_object, replica_num=0), len(contents_of_new_physical_file))
                self.assertEqual(size(session, logical_path_to_data_object, replica_num=1), len(contents_of_new_physical_file))
                self.assertNotEqual(realpath(physical_path_to_new_file), obj.replicas[0].path)
                self.assertEqual(realpath(physical_path_to_new_file), obj.replicas[1].path)
                self.assertEqual(obj.replicas[0].status, "0")
                self.assertEqual(obj.replicas[1].status, "1")

    def test_register_as_replica_with_resc_name(self):
        put_job = 'test_register_as_replica_with_resc_name.put'
        self.do_put(
            "put",
            job_name = put_job)
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        register_as_replica_job = 'test_register_as_replica_with_resc_name.register_as_replica_job'
        self.do_register_as_replica(
            "replica_with_resc_name",
            job_name = register_as_replica_job,
            resc_names = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None, job_name=register_as_replica_job)
        self.do_assert_retry_queue(count=None, job_name=register_as_replica_job)

    def test_register_as_replica_root_with_resc_name(self):
        put_job = 'test_register_as_replica_root_with_resc_name.put'
        self.do_put(
            "put",
            job_name = put_job)
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        register_as_replica_job = 'test_register_as_replica_root_with_resc_name.register_as_replica_job'
        self.do_register_as_replica(
            'replica_root_with_resc_name',
            job_name = register_as_replica_job,
            resc_names = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None, job_name=register_as_replica_job)
        self.do_assert_retry_queue(count=None, job_name=register_as_replica_job)

    def test_register_as_replica_non_leaf_non_root_with_resc_name(self):
        put_job = 'test_register_as_replica_non_leaf_non_root_with_resc_name.put'
        self.do_put(
            "put",
            job_name = put_job)
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        register_as_replica_job = 'test_register_non_leaf_non_root_with_resc_name.register_as_replica'
        event_handler = 'replica_with_non_leaf_non_root_resc_name'
        # This should result in a hierarchy error
        self.do_register_as_replica_no_assertions(event_handler, register_as_replica_job)
        self.do_assert_failed_queue(job_name=register_as_replica_job)

    def test_update_with_resc_name_with_two_replicas(self):
        put_job = 'test_update_with_resc_name_with_two_replicas.put'
        self.do_put(
            "put",
            job_name = put_job)
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        register_as_replica_job = 'test_update_with_resc_name_with_two_replicas.register_as_replica'
        self.do_register_as_replica(
            "replica_with_resc_name",
            job_name = register_as_replica_job,
            resc_names = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None, job_name=register_as_replica_job)
        self.do_assert_retry_queue(count=None, job_name=register_as_replica_job)

        update_replica_job = 'test_update_with_resc_name_with_two_replicas.update_replica'
        self.do_register_as_replica(
            "replica_with_resc_name",
            job_name = update_replica_job,
            resc_names = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None, job_name=update_replica_job)
        self.do_assert_retry_queue(count=None, job_name=update_replica_job)

    def test_update_root_with_resc_name_with_two_replicas(self):
        put_job = 'test_update_root_with_resc_name_with_two_replicas.put'
        self.do_put(
            "put",
            job_name = put_job)
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        register_as_replica_job = 'test_update_root_with_resc_name_with_two_replicas.register_as_replica'
        self.do_register_as_replica(
            "replica_root_with_resc_name",
            job_name = register_as_replica_job,
            resc_names = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None, job_name=register_as_replica_job)
        self.do_assert_retry_queue(count=None, job_name=register_as_replica_job)

        update_replica_job = 'test_update_root_with_resc_name_with_two_replicas.update_replica'
        self.do_register_as_replica(
            "replica_root_with_resc_name",
            job_name = update_replica_job,
            resc_names = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None, job_name=update_replica_job)
        self.do_assert_retry_queue(count=None, job_name=update_replica_job)

    @unittest.skip('this should result in a HIERARCHY_ERROR')
    def test_update_non_leaf_non_root_with_resc_name_with_two_replicas(self):
        put_job = 'test_update_non_leaf_non_root_with_resc_name_with_two_replicas.put'
        self.do_put(
            "put",
            job_name = put_job)
        self.do_assert_failed_queue(count=None, job_name=put_job)
        self.do_assert_retry_queue(count=None, job_name=put_job)

        register_as_replica_job = 'test_update_non_leaf_non_root_with_resc_name_with_two_replicas.register_as_replica'
        self.do_register_as_replica(
            "replica_with_non_leaf_non_root_resc_name",
            job_name = register_as_replica_job,
            resc_names = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None, job_name=register_as_replica_job)
        self.do_assert_retry_queue(count=None, job_name=register_as_replica_job)

        update_replica_job = 'test_update_non_leaf_non_root_with_resc_name_with_two_replicas.update_replica'
        self.do_register_as_replica(
            "replica_with_non_leaf_non_root_resc_name",
            job_name = update_replica_job,
            resc_names = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None, job_name=update_replica_job)
        self.do_assert_retry_queue(count=None, job_name=update_replica_job)

    def do_put_to_child(self, job_name = DEFAULT_JOB_NAME):
        with iRODSSession(**get_kwargs()) as session:
            session.resources.remove_child(REGISTER_RESC2, REGISTER_RESC2A)
        self.do_put(
            "put_with_resc_name",
            job_name = job_name,
            resc_names = [REGISTER_RESC2A],
            resc_roots = [REGISTER_RESC_PATH2A])
        with iRODSSession(**get_kwargs()) as session:
            session.resources.add_child(REGISTER_RESC2, REGISTER_RESC2A)

    # replica with another replica in hier
    def test_register_as_replica_with_resc_name_with_another_replica_in_hier(self):
        put_job = 'test_register_as_replica_with_resc_name_with_another_replica_in_hier.put'
        self.do_put_to_child(job_name = put_job)
        self.do_assert_failed_queue(count=None, job_name=put_job)

        register_as_replica_job = 'test_register_as_replica_with_resc_name_with_another_replica_in_hier.register_as_replica_job'
        self.do_register_as_replica_no_assertions(
            "replica_with_resc_name",
            job_name = register_as_replica_job)
        self.do_assert_failed_queue("wrong paths", job_name=register_as_replica_job)

    @unittest.skip('irods/irods#3517 - this is not allowed')
    def test_register_as_replica_root_with_resc_name_with_another_replica_in_hier(self):
        self.do_put_to_child()
        self.do_register_as_replica_no_assertions("replica_root_with_resc_name")
        self.do_assert_failed_queue("wrong paths", job_name=job_name)

    @unittest.skip('this should result in a HIERARCHY_ERROR')
    def test_register_as_replica_non_leaf_non_root_with_resc_name_with_another_replica_in_hier(self):
        self.do_put_to_child()
        self.do_register_as_replica_no_assertions("replica_with_non_root_non_leaf_resc_name")
        self.do_assert_failed_queue("wrong paths", job_name=job_name)


    # register with as replica event handler
    @unittest.skip('irods/irods#4623')
    def test_register_with_as_replica_event_handler_with_resc_name(self):
        self.do_register("replica_with_resc_name", resc_name = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

    @unittest.skip('irods/irods#4623')
    def test_register_with_as_replica_event_handler_root_with_resc_name(self):
        self.do_register("replica_root_with_resc_name", resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

    def test_register_with_as_replica_event_handler_non_leaf_non_root_with_resc_name(self):
        # Identical to test_register_non_leaf_non_root_with_resc_name
        pass

class Test_update_metadata(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_update_metadata, self).setUp()

    def tearDown(self):
        super(Test_update_metadata, self).tearDown()

    # update metadata
    def do_update_metadata(self, eh_name, job_name = DEFAULT_JOB_NAME, resc_name = [DEFAULT_RESC]):
        ctime_files(NFILES)
        self.do_register(eh_name, job_name, resc_name = resc_name)
        with iRODSSession(**get_kwargs()) as session:
            for i in listdir(PATH_TO_SOURCE_DIR):
                path = join(PATH_TO_SOURCE_DIR, i)
                rpath = PATH_TO_COLLECTION + "/" + i
                s1 = getsize(path)
                mtime1 = int(getmtime(path))
                s2 = size(session, rpath)
                mtime2 = modify_time(session, rpath)
                self.assertEqual(s1, s2)
                self.assertEqual(datetime.utcfromtimestamp(mtime1), mtime2)

    def test_update_metadata(self):
        register_job = 'test_update_metadata.register'
        self.do_register(
            "register",
            job_name = 'test_update_metadata.do_register')
        self.do_assert_failed_queue(count=None, job_name=register_job)
        self.do_assert_retry_queue(count=None, job_name=register_job)

        update_job = 'test_update_metadata.update'
        self.do_update_metadata(
            "register",
            job_name = update_job)
        self.do_assert_failed_queue(count=None, job_name=update_job)
        self.do_assert_retry_queue(count=None, job_name=update_job)

    def test_update_metadata_with_resc_name(self):
        register_job = 'test_update_metadata_with_resc_name.register'
        self.do_register(
            "register_with_resc_name",
            job_name = register_job,
            resc_name=[REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None, job_name=register_job)
        self.do_assert_retry_queue(count=None, job_name=register_job)

        update_metadata_job = 'test_update_metadata_with_resc_name.update_metadata'
        self.do_update_metadata(
            "register_with_resc_name",
            job_name = update_metadata_job,
            resc_name=[REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None, job_name=update_metadata_job)
        self.do_assert_retry_queue(count=None, job_name=update_metadata_job)

    def test_update_metadata_root_with_resc_name(self):
        register_job = 'test_update_metadata_root_with_resc_name.register'
        self.do_register(
            "register_root_with_resc_name",
            job_name = register_job,
            resc_name=[REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None, job_name=register_job)
        self.do_assert_retry_queue(count=None, job_name=register_job)

        update_metadata_job = 'test_update_metadata_root_with_resc_name.update_metadata'
        self.do_update_metadata(
            "register_root_with_resc_name",
            job_name = update_metadata_job,
            resc_name=[REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None, job_name=update_metadata_job)
        self.do_assert_retry_queue(count=None, job_name=update_metadata_job)

    def test_update_metadata_non_leaf_non_root_with_resc_name(self):
        # Identical to test_register_non_leaf_non_root_with_resc_name
        pass

class Test_register(automated_ingest_test_context, unittest.TestCase):
    def setUp(self):
        super(Test_register, self).setUp()

    def tearDown(self):
        super(Test_register, self).tearDown()

    def test_register(self):
        job_name = 'test_register'
        self.do_register(
            "register",
            job_name = job_name)
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

    def test_register_with_resc_name(self):
        job_name = 'test_register_with_resc_name'
        self.do_register(
            "register_with_resc_name",
            job_name = job_name,
            resc_name = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

    def test_register_root_with_resc_name(self):
        job_name = 'test_register_root_with_resc_name'
        self.do_register(
            "register_root_with_resc_name",
            job_name = job_name, resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

    def test_register_non_leaf_non_root_with_resc_name(self):
        job_name = 'test_register_non_leaf_non_root_with_resc_name'
        event_handler = 'register_non_leaf_non_root_with_resc_name'
        proc = subprocess.Popen([
            "python", "-m", IRODS_SYNC_PY, "start",
            PATH_TO_SOURCE_DIR,
            PATH_TO_COLLECTION,
            "--event_handler", event_handler_path(event_handler),
            "--job_name", job_name,
            "--log_level", "INFO",
            '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(1)
        wait_for(workers, job_name)

        # This should result in a hierarchy error
        self.do_assert_failed_queue(job_name=job_name)

    def do_register_par(self, eh_name, job_name = DEFAULT_JOB_NAME, resc_names=[DEFAULT_RESC]):
        eh = event_handler_path(eh_name)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, PATH_TO_COLLECTION, "--event_handler", eh, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(NWORKERS)
        wait_for(workers, job_name)

        self.do_assert_register(resc_names)

    def test_register_with_multiple_workers(self):
        job_name = 'test_register_with_multiple_workers'
        self.do_register_par(
            "register",
            job_name = job_name)
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

    def do_register_to_invalid_zone(self, target_collection, job_name = DEFAULT_JOB_NAME):
        subprocess.check_output(
            ["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, target_collection, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'],
            stderr=subprocess.PIPE)
        workers = start_workers(1)
        wait_for(workers, job_name)
        count_of_dir_and_files = NFILES + 1
        self.do_assert_failed_queue(count=count_of_dir_and_files, job_name=job_name)

    def test_register_to_invalid_zone(self):
        self.do_register_to_invalid_zone(
            '/invalidZone/home/rods',
            job_name = 'test_register_to_invalid_zone')

    def test_register_to_existing_zone_substring(self):
        self.do_register_to_invalid_zone(
            '/tempZ/home/rods',
            job_name = 'test_register_to_existing_zone_substring')

    def test_register_to_existing_zone_superstring(self):
        self.do_register_to_invalid_zone(
            '/tempZoneMore/home/rods',
            job_name = 'test_register_to_existing_zone_superstring')

    def test_register_to_root_collection(self):
        target_collection = '/'
        expected_err_msg = 'Root may only contain collections which represent zones'
        job_name = 'test_register_to_root_collection'
        try:
            subprocess.check_output(
                ["python", "-m", IRODS_SYNC_PY, "start", PATH_TO_SOURCE_DIR, target_collection, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'],
                stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            self.assertTrue(expected_err_msg in str(e.stderr))
            return
        else:
            self.fail('target collection should fail to ingest')

@unittest.skip('Test does not seem to raise UnicodeEncodeError')
class Test_irods_sync_UnicodeEncodeError(unittest.TestCase):
    def setUp(self):
        super(Test_register_as_replica, self).setUp()

    def tearDown(self):
        super(Test_register_as_replica, self).tearDown()

    # TODO: eh?
    def do_register_as_replica_no_assertions(self, eh, job_name = DEFAULT_JOB_NAME):
        clear_redis()

        # Create a file in a known location with an out-of-range Unicode character in the name
        bad_filename = 'test_register_with_unicode_encode_error_path_' + chr(65535)
        self.source_dir_path = join(os.path.dirname(PATH_TO_SOURCE_DIR), 'b')
        os.makedirs(self.source_dir_path)

        self.dest_coll_path = join('/tempZone/home/rods', os.path.basename(self.source_dir_path))
        self.bad_filepath = join(self.source_dir_path, bad_filename).encode('utf8')
        self.create_bad_file()

        utf8_escaped_abspath = self.bad_filepath.decode('utf8').encode('utf8', 'surrogateescape')
        self.b64_path_str = base64.b64encode(utf8_escaped_abspath)
        self.unicode_error_filename = 'irods_UnicodeEncodeError_' + str(self.b64_path_str.decode('utf8')).rstrip('/')
        self.expected_logical_path = join(self.dest_coll_path, self.unicode_error_filename)

        with iRODSSession(**get_kwargs()) as session:
            create_resources(session, HIERARCHY1)


    def tearDown(self):
        clear_redis()
        delete_collection_if_exists(self.dest_coll_path)
        rmtree(self.source_dir_path, ignore_errors=True)
        with iRODSSession(**get_kwargs()) as session:
            delete_resources(session, HIERARCHY1)

    # Helper member functions
    def assert_logical_path(self, session):
        self.assertTrue(session.collections.exists(self.dest_coll_path),
            msg='Did not find collection {self.dest_coll_path}'.format(**locals()))
        self.assertTrue(session.data_objects.exists(self.expected_logical_path),
            msg='Did not find data object {self.expected_logical_path}'.format(**locals()))

    def assert_physical_path_and_resource(self, session, expected_physical_path, expected_resource=DEFAULT_RESC):
        obj = session.data_objects.get(self.expected_logical_path)
        self.assertEqual(obj.replicas[0].path, expected_physical_path)
        self.assertEqual(obj.replicas[0].resource_name, expected_resource)

    def assert_data_object_contents(self, session):
        original_file_contents = read_file(self.bad_filepath)
        replica_file_contents = read_data_object(session, self.expected_logical_path)
        self.assertEqual(original_file_contents, replica_file_contents)

    def assert_metadata_annotation(self, session):
        obj = session.data_objects.get(self.expected_logical_path)
        metadata_value = obj.metadata.get_one('irods::automated_ingest::UnicodeEncodeError')
        self.assertEqual(str(metadata_value.value), str(self.b64_path_str.decode('utf8')))

    def assert_data_object_size(self, session):
        s1 = getsize(self.bad_filepath)
        s2 = size(session, self.expected_logical_path)
        self.assertEqual(s1, s2)

    def assert_data_object_mtime(self, session):
        mtime1 = int(getmtime(self.bad_filepath))
        mtime2 = modify_time(session, self.expected_logical_path)
        self.assertEqual(datetime.utcfromtimestamp(mtime1), mtime2)

    def do_assert_failed_queue(self, error_message=None, count=NFILES, job_name = DEFAULT_JOB_NAME):
        self.assertEqual(sync_job(job_name, get_redis()).failures_handle().get_value(), count)

    def do_assert_retry_queue(self, error_message=None, count=NFILES, job_name = DEFAULT_JOB_NAME):
        self.assertEqual(sync_job(job_name, get_redis()).retries_handle().get_value(), count)

    def create_bad_file(self):
        if os.path.exists(self.bad_filepath):
            os.unlink(self.bad_filepath)
        with open(self.bad_filepath, 'w') as f:
            f.write('Test_irods_sync_UnicodeEncodeError')

    def run_scan_with_event_handler(self, eh_name, job_name = DEFAULT_JOB_NAME):
        event_handler = event_handler_path(eh_name)
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", self.source_dir_path, self.dest_coll_path, "--event_handler", event_handler, "--job_name", job_name, "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(1)
        wait_for(workers, job_name)

    # Tests
    def test_register(self):
        expected_physical_path = join(self.source_dir_path, self.unicode_error_filename)

        job_name = 'test_register.run_scan_with_event_handler'
        self.run_scan_with_event_handler(
            "register",
            job_name = job_name)

        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

        with iRODSSession(**get_kwargs()) as session:
            self.assert_logical_path(session)
            self.assert_physical_path_and_resource(session, expected_physical_path)
            self.assert_metadata_annotation(session)
            self.assert_data_object_size(session)
            self.assert_data_object_mtime(session)

    def test_register_as_replica(self):
        expected_physical_path = join(self.source_dir_path, self.unicode_error_filename)

        job_name = 'test_register_as_replica.put'
        self.run_scan_with_event_handler(
            "put",
            job_name = job_name)
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

        clear_redis()

        self.run_scan_with_event_handler("replica_with_resc_name")

        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

        with iRODSSession(**get_kwargs()) as session:
            #import irods.keywords as kw
            #obj = session.data_objects.get(self.expected_logical_path)
            #options = {kw.REPL_NUM_KW: str(0), kw.COPIES_KW: str(1)}
            #obj.unlink(**options)

            obj = session.data_objects.get(self.expected_logical_path)
            self.assert_logical_path(session)
            #self.assert_physical_path_and_resource(session, expected_physical_path)
            self.assertEqual(obj.replicas[1].path, expected_physical_path)
            self.assertEqual(obj.replicas[1].resource_name, REGISTER_RESC2A)
            #self.assert_metadata_annotation(session)
            metadata_value = obj.metadata.get_one('irods::automated_ingest::UnicodeEncodeError')
            self.assertEqual(str(metadata_value.value), str(self.b64_path_str.decode('utf8')))
            #self.assert_data_object_size(session)
            s1 = size(session, self.expected_logical_path, replica_num=0)
            s2 = size(session, self.expected_logical_path, replica_num=1)
            self.assertEqual(s1, s2)

    def test_put(self):
        expected_physical_path = join(DEFAULT_RESC_VAULT_PATH, 'home', 'rods', os.path.basename(self.source_dir_path), self.unicode_error_filename)

        self.run_scan_with_event_handler("put")

        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

        with iRODSSession(**get_kwargs()) as session:
            self.assert_logical_path(session)
            self.assert_physical_path_and_resource(session, expected_physical_path)
            self.assert_data_object_contents(session)
            self.assert_metadata_annotation(session)
            self.assert_data_object_size(session)

    def test_put_sync(self):
        expected_physical_path = join(DEFAULT_RESC_VAULT_PATH, 'home', 'rods', os.path.basename(self.source_dir_path), self.unicode_error_filename)

        job_name = 'test_put_sync.sync'
        self.run_scan_with_event_handler(
            "sync",
            job_name = job_name)
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

        self.create_bad_file()

        self.run_scan_with_event_handler("sync")
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

        with iRODSSession(**get_kwargs()) as session:
            self.assert_logical_path(session)
            self.assert_physical_path_and_resource(session, expected_physical_path)
            self.assert_data_object_contents(session)
            self.assert_metadata_annotation(session)
            self.assert_data_object_size(session)

    def test_put_append(self):
        expected_physical_path = join(DEFAULT_RESC_VAULT_PATH, 'home', 'rods', os.path.basename(self.source_dir_path), self.unicode_error_filename)

        job_name = 'test_put_append.put'
        self.run_scan_with_event_handler(
            "append",
            job_name = job_name)
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

        with open(self.bad_filepath, 'a') as f:
            f.write('test_put_append')

        self.run_scan_with_event_handler("append")
        self.do_assert_failed_queue(count=None, job_name=job_name)
        self.do_assert_retry_queue(count=None, job_name=job_name)

        with iRODSSession(**get_kwargs()) as session:
            self.assert_logical_path(session)
            self.assert_physical_path_and_resource(session, expected_physical_path)
            self.assert_data_object_contents(session)
            self.assert_metadata_annotation(session)
            self.assert_data_object_size(session)

def main():
    unittest.main()

if __name__ == '__main__':
    main()
