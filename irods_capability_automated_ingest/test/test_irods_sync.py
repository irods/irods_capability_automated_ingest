import unittest
import os
import os.path
import stat
import glob
from unittest import TestCase
from redis import StrictRedis
from subprocess import Popen
from signal import SIGINT
from os import makedirs, listdir, remove
from shutil import rmtree
from os.path import join, realpath, getmtime, getsize, dirname, basename, relpath, isfile
from irods.session import iRODSSession
from irods.models import Collection, DataObject
from tempfile import NamedTemporaryFile
from datetime import datetime
from ..sync_utils import size, get_with_key, app, failures_key, retries_key
from ..sync_task import done
import time

LOG_FILE = "/tmp/a"

IRODS_MAJOR = 4
IRODS_MINOR = 2

IRODS_SYNC_PY = "irods_capability_automated_ingest.irods_sync"

A = "a"
A_REMOTE = "a_remote"
A_COLL = "/tempZone/home/rods/" + A_REMOTE

NFILES = 10
NWORKERS = 10
TIMEOUT = 60

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
    workers = Popen(["celery", "-A", "irods_capability_automated_ingest.sync_task", "worker", "-c", str(n), "-l", "info", "-Q", "restart,path,file"] + args)
    return workers


def start_scheduler(n):
    scheduler = Popen(["rqscheduler", "-i", "1"])
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


def read_data_object(session, path, resc_name = "demoResc"):
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
    proc = Popen(["irmtrash"])
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

    def tearDown(self):
        delete_files()
        clear_redis()
        delete_collection_if_exists(A_COLL)
        irmtrash()
        with iRODSSession(irods_env_file=env_file) as session:
            delete_resources(session, HIERARCHY1)

    def do_no_event_handler(self):
        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--log_level", "INFO"])
        proc.wait()
        self.do_register2()

    def do_register(self, eh, resc_name = ["demoResc"]):
        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        self.do_register2(resc_names=resc_name)

    def do_register2(self, resc_names=["demoResc"]):
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

    def do_register_dir_par(self, eh, resc_names=["demoResc"]):
        create_files2(10, NFILES)
        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
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

    def do_register_par(self, eh, resc_names=["demoResc"]):
        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(NWORKERS)
        wait_for(workers)

        self.do_assert_register(resc_names)

    def do_retry(self, eh, resc_name = ["demoResc"]):
        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(1)
        wait_for(workers)

        r = StrictRedis()

        self.do_assert_failed_queue("no failures", count=None)
        self.do_assert_retry_queue()

    def do_no_retry(self, eh, resc_name = ["demoResc"]):
        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()
        workers = start_workers(1)
        wait_for(workers)

        self.do_assert_failed_queue("no failures")

    def do_put(self, eh, resc_names = ["demoResc"], resc_roots = ["/var/lib/irods/Vault"]):
        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)

        self.do_assert_put(resc_names, resc_roots)

    def do_put_par(self, eh, resc_names=["demoResc"], resc_roots=["/var/lib/irods/Vault"]):
        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
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
                vaultpaths = map(lambda resc_root : resc_root + "/home/rods/" + A_REMOTE + "/" + i, resc_roots)
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

        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)

    def do_register_as_replica(self, eh, resc_names = ["demoResc"]):
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

    def do_update(self, eh, resc_name = ["demoResc"]):
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

    def do_update_metadata(self, eh, resc_name = ["demoResc"]):
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

        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
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

        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)

    def do_pre_job(self, eh):
        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)
        with open("/tmp/a","r") as f:
            lines = f.readlines()
            self.assertEqual(lines, ["pre_job"])


    def do_post_job(self, eh):
        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)
        with open("/tmp/a","r") as f:
            lines = f.readlines()
            self.assertEqual(lines, ["post_job"])

    def do_timeout(self, eh):
        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
        proc.wait()

        workers = start_workers(1)
        wait_for(workers)

        r = StrictRedis()
        self.do_assert_failed_queue(count=10)

    def do_append_json(self, eh):
        recreate_files(NFILES)

        proc = Popen(["python", "-m", IRODS_SYNC_PY, "start", A, A_COLL, "--event_handler", eh, "--append_json", "\"append_json\"", "--job_name", "test_irods_sync", "--log_level", "INFO", '--files_per_task', '1'])
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


if __name__ == '__main__':
        unittest.main()
