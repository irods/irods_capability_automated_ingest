import unittest
import os
import os.path
import stat
import subprocess
import glob
import base64
from unittest import TestCase
from redis import StrictRedis
from signal import SIGINT
from os import makedirs, listdir, remove
from shutil import rmtree
from os.path import join, realpath, getmtime, getsize, dirname, basename, relpath, isfile
from irods.session import iRODSSession
from irods.models import Collection, DataObject
from tempfile import NamedTemporaryFile, mkdtemp
from datetime import datetime
from ..sync_utils import size, get_with_key, app, failures_key, retries_key
from ..sync_task import done
import time

LOG_FILE = "/tmp/a"

IRODS_MAJOR = 4
IRODS_MINOR = 2

ZONENAME = "tempZone"
RODSADMIN = "rods"

IRODS_SYNC_PY = "irods_capability_automated_ingest.irods_sync"

A = "a"
A_REMOTE = "a_remote"
A_COLL = "/" + ZONENAME + "/home/" + RODSADMIN + "/" + A_REMOTE

NFILES = 10
NWORKERS = 10
TIMEOUT = 60

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
            "host": "localhost",
            "path": REGISTER_RESC_PATH
        }
    },
    PUT_RESC : {
        "type" : "unixfilesystem",
        "kwargs" : {
            "host": "localhost",
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
                            "host": "localhost",
                            "path": REGISTER_RESC_PATH2A
                        }
                    },
                    REGISTER_RESC2B: {
                        "type": "unixfilesystem",
                        "kwargs": {
                            "host": "localhost",
                            "path": REGISTER_RESC_PATH2B
                        }
                    },
                }
            }
        }
    }
}

def clear_redis():
    r = StrictRedis()
    r.flushdb()


def start_workers(n, args=[]):
    os.environ["CELERY_BROKER_URI"] = "redis://localhost/0"
    print("start " + str(n) + " worker(s)")
    workers = subprocess.Popen(["celery", "-A", "irods_capability_automated_ingest.sync_task", "worker", "-c", str(n), "-l", "info", "-Q", "restart,path,file"] + args)
    return workers


def start_scheduler(n):
    scheduler = subprocess.Popen(["rqscheduler", "-i", "1"])
    return scheduler


def wait_for(workers):
    r = StrictRedis()
    t0 = time.time()
    while TIMEOUT is None or time.time() - t0 < TIMEOUT:
        restart = r.llen("restart")
        i = app.control.inspect()
        act = i.active()
        if act is None:
            active = 0
        else:
            active = sum(map(len, act.values()))
        d = done(r, "test_irods_sync")
        print ("restart = " + str(restart) + ", active = " + str(active) + ", done = " + str(d) + ", t0 = " + str(t0) + ", t = " + str(time.time()))
        if restart != 0 or active != 0 or not d:
            time.sleep(1)
        else:
            break

    workers.send_signal(SIGINT)
    workers.wait()


def interrupt(scheduler):
    scheduler.send_signal(SIGINT)

    scheduler.wait()


def create_files(nfiles):
    create_files2(0, nfiles)


def create_files2(depth, nfiles):
    a = join(A, *list(map(lambda i: "a" + str(i), range(depth))))
    makedirs(a)
    for i in range(nfiles):
        with open(join(a, str(i)), "w") as f:
            f.write("i" * i)


def recreate_files(nfiles):
    recreate_files2(0, nfiles)


def recreate_files2(depth, nfiles):
    a = join(A, *list(map(lambda i: "a" + str(i), range(depth))))
    for i in range(nfiles):
        with open(join(a, str(i)), "w") as f:
            f.write("i" * (i * 2 + 1))


def ctime_files(nfiles=NFILES):
    for i in range(nfiles):
        os.chmod(join(A, str(i)), stat.S_IRUSR )
        os.chmod(join(A, str(i)), stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)


def delete_files():
    rmtree(A)
    if isfile(LOG_FILE):
        remove(LOG_FILE)


def read_file(path):
    with open(path) as f:
        return f.read()


def read_data_object(session, path, resc_name = DEFAULT_RESC):
    with NamedTemporaryFile() as tf:
        session.data_objects.get(path, file=tf.name, forceFlag="", rescName = resc_name)
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
    proc = subprocess.Popen(["irmtrash"])
    proc.wait()


def delete_collection(coll):
    with iRODSSession(irods_env_file=env_file) as session:
        session.collections.remove(coll)


def delete_collection_if_exists(coll):
    with iRODSSession(irods_env_file=env_file) as session:
        if(session.collections.exists(coll)):
           session.collections.remove(coll)


def modify_time(session, path):
    for row in session.query(DataObject.modify_time).filter(Collection.name == dirname(path), DataObject.name == basename(path)):
        return row[DataObject.modify_time]


try:
    env_file = os.environ['IRODS_ENVIRONMENT_FILE']
except KeyError:
    env_file = os.path.expanduser('~/.irods/irods_environment.json')


class Test_irods_sync(TestCase):
    def setUp(self):
        clear_redis()
        create_files(NFILES)
        delete_collection_if_exists(A_COLL)
        with iRODSSession(irods_env_file=env_file) as session:
            create_resources(session, HIERARCHY1)

        self.logfile = NamedTemporaryFile()

    def tearDown(self):
        delete_files()
        clear_redis()
        delete_collection_if_exists(A_COLL)
        irmtrash()
        with iRODSSession(irods_env_file=env_file) as session:
            delete_resources(session, HIERARCHY1)

    def do_no_event_handler(self):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--log_level", "INFO"])
        proc.wait()
        self.do_register2()

    def do_register(self, eh, resc_name = [DEFAULT_RESC]):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        self.do_register2(resc_names=resc_name)

    def do_register2(self, resc_names=[DEFAULT_RESC]):
        workers = start_workers(1)
        wait_for(workers)

        self.do_assert_register(resc_names)

    def do_assert_register(self, resc_names):

        with iRODSSession(irods_env_file=env_file) as session:
            self.assertTrue(session.collections.exists(A_COLL))
            for i in listdir(A):
                path = join(A, i)
                rpath = A_COLL + "/" + i
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

    def do_register_dir_par(self, eh, resc_names=[DEFAULT_RESC]):
        create_files2(10, NFILES)
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(NWORKERS)
        wait_for(workers)

        with iRODSSession(irods_env_file=env_file) as session:
            self.assertTrue(session.collections.exists(A_COLL))
            for i in glob.glob(A+"/**/*", recursive=True):
                if isfile(i):
                    path = i
                    rpath = A_COLL + "/" + relpath(i, A)
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

    def do_register_par(self, eh, resc_names=[DEFAULT_RESC]):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(NWORKERS)
        wait_for(workers)

        self.do_assert_register(resc_names)

    def do_retry(self, eh, resc_name = [DEFAULT_RESC]):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(1)
        wait_for(workers)

        r = StrictRedis()

        self.do_assert_failed_queue("no failures", count=None)
        self.do_assert_retry_queue()

    def do_no_retry(self, eh, resc_name = [DEFAULT_RESC]):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(1)
        wait_for(workers)

        self.do_assert_failed_queue("no failures")

    def do_put(self, eh, resc_names = [DEFAULT_RESC], resc_roots = [DEFAULT_RESC_VAULT_PATH]):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)

        self.do_assert_put(resc_names, resc_roots)

    def do_put_par(self, eh, resc_names=[DEFAULT_RESC], resc_roots=[DEFAULT_RESC_VAULT_PATH]):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(NWORKERS)
        wait_for(workers)

        self.do_assert_put(resc_names, resc_roots)

    def do_assert_put(self, resc_names, resc_roots):

        with iRODSSession(irods_env_file=env_file) as session:
            self.assertTrue(session.collections.exists(A_COLL))
            for i in listdir(A):
                path = join(A, i)
                rpath = A_COLL + "/" + i
                vaultpaths = map(lambda resc_root : resc_root + "/home/" + RODSADMIN + "/" + A_REMOTE + "/" + i, resc_roots)
                self.assertTrue(session.data_objects.exists(rpath))
                a1 = read_file(path)

                a2 = read_data_object(session, rpath)
                self.assertEqual(a1, a2)

                obj = session.data_objects.get(rpath)
                self.assertIn(obj.replicas[0].path, vaultpaths)
                self.assertIn(obj.replicas[0].resource_name, resc_names)

    def do_register_as_replica_no_assertions(self, eh):
        clear_redis()
        recreate_files(NFILES)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)

    def do_register_as_replica(self, eh, resc_names = [DEFAULT_RESC]):
        self.do_register_as_replica_no_assertions(eh)
        with iRODSSession(irods_env_file=env_file) as session:
            self.assertTrue(session.collections.exists(A_COLL))
            for i in listdir(A):
                path = join(A, i)
                rpath = A_COLL + "/" + i
                self.assertTrue(session.data_objects.exists(rpath))
                a1 = read_file(path)

                obj = session.data_objects.get(rpath)
                self.assertEqual(len(obj.replicas), 2)
                resc_name_replica_1 = obj.replicas[1].resource_name
                self.assertIn(resc_name_replica_1, resc_names)
                a2 = read_data_object(session, rpath, resc_name = resc_name_replica_1)
                self.assertEqual(a1, a2)
                a2 = read_data_object(session, rpath)
                self.assertEqual(a1, a2)
                self.assertNotEqual(size(session, rpath, replica_num=0), len(a1))
                self.assertEqual(size(session, rpath, replica_num=1), len(a1))
                self.assertNotEqual(realpath(path), obj.replicas[0].path)
                self.assertEqual(realpath(path), obj.replicas[1].path)
                # self.assertEqual(obj.replicas[0].status, "0")
                # self.assertEqual(obj.replicas[1].status, "1")

    def do_update(self, eh, resc_name = [DEFAULT_RESC]):
        recreate_files(NFILES)
        self.do_register(eh, resc_name = resc_name)
        with iRODSSession(irods_env_file=env_file) as session:
            for i in listdir(A):
                path = join(A, i)
                rpath = A_COLL + "/" + i
                s1 = getsize(path)
                mtime1 = int(getmtime(path))
                s2 = size(session, rpath)
                mtime2 = modify_time(session, rpath)
                self.assertEqual(s1, s2)
                self.assertEqual(datetime.utcfromtimestamp(mtime1), mtime2)

    def do_update_metadata(self, eh, resc_name = [DEFAULT_RESC]):
        ctime_files(NFILES)
        self.do_register(eh, resc_name = resc_name)
        with iRODSSession(irods_env_file=env_file) as session:
            for i in listdir(A):
                path = join(A, i)
                rpath = A_COLL + "/" + i
                s1 = getsize(path)
                mtime1 = int(getmtime(path))
                s2 = size(session, rpath)
                mtime2 = modify_time(session, rpath)
                self.assertEqual(s1, s2)
                self.assertEqual(datetime.utcfromtimestamp(mtime1), mtime2)

    def do_no_sync(self, eh):
        recreate_files(NFILES)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)

        with iRODSSession(irods_env_file=env_file) as session:
            for i in listdir(A):
                path = join(A, i)
                rpath = A_COLL + "/" + i
                a1 = read_file(path)

                a2 = read_data_object(session, rpath)
                self.assertNotEqual(a1, a2)

    def do_no_op(self, eh):
        recreate_files(NFILES)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)

    def do_pre_job(self, eh):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)
        with open("/tmp/a","r") as f:
            lines = f.readlines()
            self.assertEqual(lines, ["pre_job"])


    def do_post_job(self, eh):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)
        with open("/tmp/a","r") as f:
            lines = f.readlines()
            self.assertEqual(lines, ["post_job"])

    def do_timeout(self, eh):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)

        r = StrictRedis()
        self.do_assert_failed_queue(count=10)

    def do_append_json(self, eh):
        recreate_files(NFILES)

        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--append_json", "\"append_json\"", "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)

        r = StrictRedis()
        self.do_assert_failed_queue(count=None)

    def do_put_to_child(self):
        with iRODSSession(irods_env_file=env_file) as session:
            session.resources.remove_child(REGISTER_RESC2, REGISTER_RESC2A)
        self.do_put("irods_capability_automated_ingest.examples.put_with_resc_name", resc_names = [REGISTER_RESC2A], resc_roots = [REGISTER_RESC_PATH2A])
        with iRODSSession(irods_env_file=env_file) as session:
            session.resources.add_child(REGISTER_RESC2, REGISTER_RESC2A)

    def do_assert_failed_queue(self, error_message=None, count=NFILES):
        r = StrictRedis()
        self.assertEqual(get_with_key(r, failures_key, "test_irods_sync", int), count)
#         for job in rq.jobs:
#             self.assertIn(errmsg, job.exc_info)

    def do_assert_retry_queue(self, error_message=None, count=NFILES):
        r = StrictRedis()
        self.assertEqual(get_with_key(r, retries_key, "test_irods_sync", int), count)

#         for job in rq.jobs:
#             self.assertIn(errmsg, job.exc_info)

    def do_run_invalid_target_collection_test(self, target_collection, expected_err_msg):
        try:
            subprocess.check_output(
                ["python", "-m", IRODS_SYNC_PY, "start", A, target_collection, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'],
                stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            self.assertTrue(expected_err_msg in str(e.stderr))
            return
        self.fail('target collection should fail to ingest')

    # no event handler

    def test_no_event_handler(self):
        self.do_no_event_handler()
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # register

    def test_register(self):
        self.do_register("irods_capability_automated_ingest.examples.register")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_register_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.register_with_resc_name", resc_name = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_register_root_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.register_root_with_resc_name", resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_register_non_leaf_non_root_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.register_non_leaf_non_root_with_resc_name", resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_register_to_invalid_zone(self):
        self.do_run_invalid_target_collection_test(
            '/invalidZone/home/rods',
            'Invalid zone name in destination collection path')

    def test_register_to_existing_zone_substring(self):
        self.do_run_invalid_target_collection_test(
            '/tempZ/home/rods',
            'Invalid zone name in destination collection path')

    def test_register_to_existing_zone_superstring(self):
        self.do_run_invalid_target_collection_test(
            '/tempZoneMore/home/rods',
            'Invalid zone name in destination collection path')

    def test_register_to_root_collection(self):
        self.do_run_invalid_target_collection_test(
            '/',
            'Root may only contain collections which represent zones')

    # update

    def test_update(self):
        self.do_register("irods_capability_automated_ingest.examples.register")
        self.do_update("irods_capability_automated_ingest.examples.register")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_update_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.register_with_resc_name", resc_name = [REGISTER_RESC2A])
        self.do_update("irods_capability_automated_ingest.examples.register_with_resc_name", resc_name = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_update_root_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.register_root_with_resc_name", resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_update("irods_capability_automated_ingest.examples.register_root_with_resc_name", resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_update_non_leaf_non_root_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.register_non_root_non_leaf_with_resc_name", resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_update("irods_capability_automated_ingest.examples.register_non_root_non_leaf_with_resc_name", resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # update metadata

    def test_update_metadata(self):
        self.do_register("irods_capability_automated_ingest.examples.register")
        self.do_update_metadata("irods_capability_automated_ingest.examples.register")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_update_metadata_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.register_with_resc_name", resc_name=[REGISTER_RESC2A])
        self.do_update_metadata("irods_capability_automated_ingest.examples.register_with_resc_name", resc_name=[REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_update_metadata_root_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.register_root_with_resc_name", resc_name=[REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_update_metadata("irods_capability_automated_ingest.examples.register_root_with_resc_name", resc_name=[REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_update_metadata_non_leaf_non_root_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.register_non_leaf_non_root_with_resc_name", resc_name=[REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_update_metadata("irods_capability_automated_ingest.examples.register_non_leaf_non_root_with_resc_name", resc_name=[REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # replica

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_register_as_replica_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.put")
        self.do_register_as_replica("irods_capability_automated_ingest.examples.replica_with_resc_name", resc_names = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_register_as_replica_root_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.put")
        self.do_register_as_replica("irods_capability_automated_ingest.examples.replica_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_register_as_replica_non_leaf_non_root_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.put")
        self.do_register_as_replica("irods_capability_automated_ingest.examples.replica_non_leaf_non_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # update with two replicas

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_update_with_resc_name_with_two_replicas(self):
        self.do_put("irods_capability_automated_ingest.examples.put")
        self.do_register_as_replica("irods_capability_automated_ingest.examples.replica_with_resc_name", resc_names = [REGISTER_RESC2A])
        self.do_register_as_replica("irods_capability_automated_ingest.examples.replica_with_resc_name", resc_names = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_update_root_with_resc_name_with_two_replicas(self):
        self.do_put("irods_capability_automated_ingest.examples.put")
        self.do_register_as_replica("irods_capability_automated_ingest.examples.replica_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_register_as_replica("irods_capability_automated_ingest.examples.replica_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_update_non_leaf_non_root_with_resc_name_with_two_replicas(self):
        self.do_put("irods_capability_automated_ingest.examples.put")
        self.do_register_as_replica("irods_capability_automated_ingest.examples.replica_non_leaf_non_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_register_as_replica("irods_capability_automated_ingest.examples.replica_non_leaf_non_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # replica with another replica in hier

    def test_register_as_replica_with_resc_name_with_another_replica_in_hier(self):
        self.do_put_to_child()
        self.do_register_as_replica_no_assertions("irods_capability_automated_ingest.examples.replica_with_resc_name")
        self.do_assert_failed_queue("wrong paths")

    def test_register_as_replica_root_with_resc_name_with_another_replica_in_hier(self):
        self.do_put_to_child()
        self.do_register_as_replica_no_assertions("irods_capability_automated_ingest.examples.replica_root_with_resc_name")
        self.do_assert_failed_queue("wrong paths")

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_register_as_replica_non_leaf_non_root_with_resc_name_with_another_replica_in_hier(self):
        self.do_put_to_child()
        self.do_register_as_replica_no_assertions("irods_capability_automated_ingest.examples.replica_non_root_non_leaf_with_resc_name")
        self.do_assert_failed_queue("wrong paths")

    # register with as replica event handler

    def test_register_with_as_replica_event_handler_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.replica_with_resc_name", resc_name = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_register_with_as_replica_event_handler_root_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.replica_root_with_resc_name", resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_register_with_as_replica_event_handler_non_leaf_non_root_with_resc_name(self):
        self.do_register("irods_capability_automated_ingest.examples.replica_non_leaf_non_root_with_resc_name", resc_name = [REGISTER_RESC2A, REGISTER_RESC2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # put

    def test_put(self):
        self.do_put("irods_capability_automated_ingest.examples.put")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_put_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.put_with_resc_name", resc_names = [REGISTER_RESC2A], resc_roots = [REGISTER_RESC2A])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_put_root_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.put_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B], resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_put_non_leaf_non_root_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.put_non_leaf_non_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B], resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # no sync

    def test_no_sync(self):
        self.do_put("irods_capability_automated_ingest.examples.put")
        self.do_no_sync("irods_capability_automated_ingest.examples.put")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_no_sync_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.put_with_resc_name", resc_names = [REGISTER_RESC2A], resc_roots = [REGISTER_RESC2A])
        self.do_no_sync("irods_capability_automated_ingest.examples.put_with_resc_name")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_no_sync_root_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.put_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B], resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_no_sync("irods_capability_automated_ingest.examples.put_with_resc_name")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_no_sync_non_leaf_non_root_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.put_non_leaf_non_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B], resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_no_sync("irods_capability_automated_ingest.examples.put_with_resc_name")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # sync

    def test_sync(self):
        self.do_put("irods_capability_automated_ingest.examples.sync")
        recreate_files(NFILES)
        self.do_put("irods_capability_automated_ingest.examples.sync")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_sync_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.sync_with_resc_name", resc_names = [REGISTER_RESC2A], resc_roots = [REGISTER_RESC_PATH2A])
        recreate_files(NFILES)
        self.do_put("irods_capability_automated_ingest.examples.sync_with_resc_name", resc_names = [REGISTER_RESC2A], resc_roots = [REGISTER_RESC_PATH2A])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_sync_root_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.sync_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B], resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        recreate_files(NFILES)
        self.do_put("irods_capability_automated_ingest.examples.sync_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B], resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_sync_non_leaf_non_root_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.sync_non_leaf_non_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B], resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        recreate_files(NFILES)
        self.do_put("irods_capability_automated_ingest.examples.sync_non_leaf_non_root_with_resc_name", resc_names = [REGISTER_RESC2A, REGISTER_RESC2B], resc_roots = [REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # append

    def test_append(self):
        self.do_put("irods_capability_automated_ingest.examples.append")
        recreate_files(NFILES)
        self.do_put("irods_capability_automated_ingest.examples.append")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_append_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.append_with_resc_name", resc_names = [REGISTER_RESC2A], resc_roots = [REGISTER_RESC_PATH2A])
        recreate_files(NFILES)
        self.do_put("irods_capability_automated_ingest.examples.append_with_resc_name", resc_names = [REGISTER_RESC2A], resc_roots = [REGISTER_RESC_PATH2A])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    def test_append_root_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.append_root_with_resc_name", resc_names=[REGISTER_RESC2A, REGISTER_RESC2B],
                    resc_roots=[REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        recreate_files(NFILES)
        self.do_put("irods_capability_automated_ingest.examples.append_root_with_resc_name", resc_names=[REGISTER_RESC2A, REGISTER_RESC2B],
                    resc_roots=[REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_append_non_leaf_non_root_with_resc_name(self):
        self.do_put("irods_capability_automated_ingest.examples.append_non_leaf_non_root_with_resc_name", resc_names=[REGISTER_RESC2A, REGISTER_RESC2B],
                    resc_roots=[REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        recreate_files(NFILES)
        self.do_put("irods_capability_automated_ingest.examples.append_non_leaf_non_root_with_resc_name", resc_names=[REGISTER_RESC2A, REGISTER_RESC2B],
                    resc_roots=[REGISTER_RESC_PATH2A, REGISTER_RESC_PATH2B])
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # no op
    def test_no_op(self):
        self.do_no_op("irods_capability_automated_ingest.examples.no_op")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)


    # append_json
    def test_append_json(self):
        self.do_append_json("irods_capability_automated_ingest.examples.append_json")


    # create dir
    def test_create_dir(self):
        self.do_register_dir_par("irods_capability_automated_ingest.examples.register")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # register par
    def test_register_par(self):
        self.do_register_par("irods_capability_automated_ingest.examples.register")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # put par
    def test_put_par(self):
        self.do_put_par("irods_capability_automated_ingest.examples.put")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

    # retry
    def test_no_retry(self):
        self.do_no_retry("irods_capability_automated_ingest.examples.no_retry")

    def test_retry(self):
        self.do_retry("irods_capability_automated_ingest.examples.retry")


    # timeout
    def test_timeout(self):
        self.do_timeout("irods_capability_automated_ingest.examples.timeout")


    # pre and post job
    def test_pre_job(self):
        self.do_pre_job("irods_capability_automated_ingest.examples.pre_job")


    def test_post_job(self):
        self.do_post_job("irods_capability_automated_ingest.examples.post_job")

    # pep callbacks
    def run_sync_job_with_pep_callbacks(self, source_dir=A, destination_coll=A_COLL):
        eh = 'irods_capability_automated_ingest.examples.register_with_peps'
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", source_dir, destination_coll, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--log_filename', self.logfile.name, '--files_per_task', '1'])
        proc.wait()

    def assert_pep_messages_in_log(self, log_contents, messages):
        # Ensure that the list of pep messages appears in the log
        for msg in messages:
            count = log_contents.count(msg)
            self.assertEqual(1, count, msg='found {0} occurrences of message:[{1}]'.format(count, msg))

    def test_create_peps(self):
        self.run_sync_job_with_pep_callbacks()
        self.do_register2(DEFAULT_RESC)

        with open(self.logfile.name, 'r') as f:
            log_contents = f.read()

        files = [os.path.join(A_COLL, str(x)) for x in range(NFILES)]
        pep_messages = [['pre_data_obj_create:[' + filepath + ']' for filepath in files],
                        ['post_data_obj_create:[' + filepath + ']' for filepath in files],
                        ['pre_coll_create:[' + A_COLL + ']'],
                        ['post_coll_create:[' + A_COLL + ']']]
        for messages in pep_messages:
            self.assert_pep_messages_in_log(log_contents, messages)

    def test_modify_peps(self):
        # register directory A
        self.run_sync_job_with_pep_callbacks()
        self.do_register2(DEFAULT_RESC)

        # recreate files and register sync to trigger modify behavior
        rmtree(A)
        create_files(NFILES)
        self.run_sync_job_with_pep_callbacks()
        self.do_register2(DEFAULT_RESC)

        # Read in log and verify that PEPs fired
        with open(self.logfile.name, 'r') as f:
            log_contents = f.read()
        files = [os.path.join(A_COLL, str(x)) for x in range(NFILES)]
        pep_messages = [['pre_data_obj_modify:[' + filepath + ']' for filepath in files],
                        ['post_data_obj_modify:[' + filepath + ']' for filepath in files],
                        ['pre_coll_modify:[' + A_COLL + ']'],
                        ['post_coll_modify:[' + A_COLL + ']']]
        for messages in pep_messages:
            self.assert_pep_messages_in_log(log_contents, messages)

    def test_empty_coll_create_peps(self):
        empty_dir_tree = mkdtemp()
        subdir_names = ['a', 'b', 'c']
        for subdir in subdir_names:
            os.mkdir(os.path.join(empty_dir_tree, subdir))

        try:
            self.run_sync_job_with_pep_callbacks(empty_dir_tree)
            workers = start_workers(1)
            wait_for(workers)
            # Assert that the collections were created
            with iRODSSession(irods_env_file=env_file) as session:
                self.assertTrue(session.collections.exists(A_COLL))
                for subdir in subdir_names:
                    self.assertTrue(session.collections.exists(os.path.join(A_COLL, subdir)))

            with open(self.logfile.name, 'r') as f:
                log_contents = f.read()

            collections = [os.path.join(A_COLL, subdir) for subdir in subdir_names]
            pep_messages = [['pre_coll_create:[' + collection + ']' for collection in collections],
                            ['post_coll_create:[' + collection + ']' for collection in collections]]

            for messages in pep_messages:
                self.assert_pep_messages_in_log(log_contents, messages)

        finally:
            rmtree(empty_dir_tree, ignore_errors=True)

class Test_irods_sync_UnicodeEncodeError(TestCase):
    def setUp(self):
        clear_redis()
        with iRODSSession(irods_env_file=env_file) as session:
            create_resources(session, HIERARCHY1)

        # Create a file in a known location with an out-of-range Unicode character in the name
        bad_filename = 'test_register_with_unicode_encode_error_path_' + chr(65535)
        self.source_dir_path = mkdtemp()
        self.dest_coll_path = join('/tempZone/home/rods', os.path.basename(self.source_dir_path))
        self.bad_filepath = join(self.source_dir_path, bad_filename).encode('utf8')
        self.create_bad_file()

        utf8_escaped_abspath = self.bad_filepath.decode('utf8').encode('utf8', 'surrogateescape')
        self.b64_path_str = base64.b64encode(utf8_escaped_abspath)
        self.unicode_error_filename = 'irods_UnicodeEncodeError_' + str(self.b64_path_str.decode('utf8'))
        self.expected_logical_path = join(self.dest_coll_path, self.unicode_error_filename)

    def tearDown(self):
        clear_redis()
        delete_collection_if_exists(self.dest_coll_path)
        rmtree(self.source_dir_path, ignore_errors=True)
        with iRODSSession(irods_env_file=env_file) as session:
            delete_resources(session, HIERARCHY1)

    # Helper member functions
    def assert_logical_path(self, session):
        self.assertTrue(session.collections.exists(self.dest_coll_path))
        self.assertTrue(session.data_objects.exists(self.expected_logical_path))

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

    def do_assert_failed_queue(self, error_message=None, count=NFILES):
        r = StrictRedis()
        self.assertEqual(get_with_key(r, failures_key, "test_irods_sync", int), count)

    def do_assert_retry_queue(self, error_message=None, count=NFILES):
        r = StrictRedis()
        self.assertEqual(get_with_key(r, retries_key, "test_irods_sync", int), count)

    def create_bad_file(self):
        if os.path.exists(self.bad_filepath):
            os.unlink(self.bad_filepath)
        with open(self.bad_filepath, 'a') as f:
            f.write('Test_irods_sync_UnicodeEncodeError')

    def run_scan_with_event_handler(self, event_handler):
        proc = subprocess.Popen(["python", "-m", IRODS_SYNC_PY, "start", self.source_dir_path, self.dest_coll_path, "--event_handler", event_handler, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(1)
        wait_for(workers)

    # Tests
    def test_register(self):
        expected_physical_path = join(self.source_dir_path, self.unicode_error_filename)

        self.run_scan_with_event_handler("irods_capability_automated_ingest.examples.register")
 
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

        with iRODSSession(irods_env_file=env_file) as session:
            self.assert_logical_path(session)
            self.assert_physical_path_and_resource(session, expected_physical_path)
            self.assert_metadata_annotation(session)
            self.assert_data_object_size(session)
            self.assert_data_object_mtime(session)

    @unittest.skipIf(IRODS_MAJOR < 4 or (IRODS_MAJOR == 4 and IRODS_MINOR < 3), "skip")
    def test_register_as_replica(self):
        expected_physical_path = join(self.source_dir_path, self.unicode_error_filename)

        self.run_scan_with_event_handler("irods_capability_automated_ingest.examples.put")

        clear_redis()

        self.run_scan_with_event_handler("irods_capability_automated_ingest.examples.replica_with_resc_name")

        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

        with iRODSSession(irods_env_file=env_file) as session:
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

        self.run_scan_with_event_handler("irods_capability_automated_ingest.examples.put")

        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

        with iRODSSession(irods_env_file=env_file) as session:
            self.assert_logical_path(session)
            self.assert_physical_path_and_resource(session, expected_physical_path)
            self.assert_data_object_contents(session)
            self.assert_metadata_annotation(session)
            self.assert_data_object_size(session)

    def test_put_sync(self):
        expected_physical_path = join(DEFAULT_RESC_VAULT_PATH, 'home', 'rods', os.path.basename(self.source_dir_path), self.unicode_error_filename)

        self.run_scan_with_event_handler("irods_capability_automated_ingest.examples.sync")

        self.create_bad_file()

        self.run_scan_with_event_handler("irods_capability_automated_ingest.examples.sync")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

        with iRODSSession(irods_env_file=env_file) as session:
            self.assert_logical_path(session)
            self.assert_physical_path_and_resource(session, expected_physical_path)
            self.assert_data_object_contents(session)
            self.assert_metadata_annotation(session)
            self.assert_data_object_size(session)

    def test_put_append(self):
        expected_physical_path = join(DEFAULT_RESC_VAULT_PATH, 'home', 'rods', os.path.basename(self.source_dir_path), self.unicode_error_filename)

        self.run_scan_with_event_handler("irods_capability_automated_ingest.examples.append")

        with open(self.bad_filepath, 'a') as f:
            f.write('test_put_append')

        self.run_scan_with_event_handler("irods_capability_automated_ingest.examples.append")
        self.do_assert_failed_queue(count=None)
        self.do_assert_retry_queue(count=None)

        with iRODSSession(irods_env_file=env_file) as session:
            self.assert_logical_path(session)
            self.assert_physical_path_and_resource(session, expected_physical_path)
            self.assert_data_object_contents(session)
            self.assert_metadata_annotation(session)
            self.assert_data_object_size(session)


if __name__ == '__main__':
        unittest.main()
