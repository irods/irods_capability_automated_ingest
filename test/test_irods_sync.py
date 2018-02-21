import unittest
import os
import os.path
from unittest import TestCase
from redis import StrictRedis
from subprocess import Popen, DEVNULL, PIPE
from signal import SIGINT
from os import makedirs, listdir, environ
from shutil import rmtree
from os.path import join, realpath, getmtime, getsize, dirname, basename
from irods.session import iRODSSession
from irods.models import Collection, DataObject
from tempfile import NamedTemporaryFile
from datetime import datetime

A = "a"
A_REMOTE = "a_remote"
A_COLL = "/tempZone/home/rods/" + A_REMOTE

NFILES = 10

REGISTER_RESC = "regiResc"
REGISTER_RESC_PATH = "/var/lib/irods/Vault2"

PUT_RESC = "putResc"
PUT_RESC_PATH = "/var/lib/irods/Vault3"

def clear_redis():
    r = StrictRedis()
    r.flushdb()

def start_workers(n):
    workers = map(lambda x: Popen(["rq", "worker", "--burst", "restart", "path", "file"]), range(n))

    return workers
    
def start_scheduler(n):
    scheduler = Popen(["rqscheduler", "-i", "1"])
    return scheduler

def wait(workers):
    for worker in workers:
        worker.wait()

def interrupt(scheduler):
    scheduler.send_signal(SIGINT)

    scheduler.wait()

def create_files():
    makedirs(A)
    for i in range(NFILES):
        with open(join(A,str(i)), "w") as f:
            f.write("i" * i)

def recreate_files():
    for i in range(NFILES):
        with open(join(A,str(i)), "w") as f:
            f.write("i" * (i * 2 + 1))

def delete_files():
    rmtree(A)

def read_file(path):
    with open(path) as f:
        return f.read()

def read_data_object(session, path, resc_name = "demoResc"):
    with NamedTemporaryFile() as tf:
        session.data_objects.get(path, file=tf.name, forceFlag="", rescName = resc_name)
        return read_file(tf.name)

def create_resources():
    with iRODSSession(irods_env_file=env_file) as session:
        session.resources.create(REGISTER_RESC, "unixfilesystem", host="localhost", path=REGISTER_RESC_PATH)
        session.resources.create(PUT_RESC, "unixfilesystem", host="localhost", path=PUT_RESC_PATH)

def delete_resources():
    with iRODSSession(irods_env_file=env_file) as session:
        session.resources.remove(REGISTER_RESC)
        session.resources.remove(PUT_RESC)
    
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
    
def size(session, path, replica_num = None, resc_name = None):
    args = [Collection.name == dirname(path), DataObject.name == basename(path)]
    
    if replica_num is not None:
        args.append(DataObject.replica_number == replica_num)
        
    if resc_name is not None:
        args.append(DataObject.resource_name == resc_name)
        
    for row in session.query(DataObject.size).filter(*args):
        return int(row[DataObject.size])

try:
    env_file = os.environ['IRODS_ENVIRONMENT_FILE']
except KeyError:
    env_file = os.path.expanduser('~/.irods/irods_environment.json')

class Test_irods_sync(TestCase):
    def setUp(self):
        clear_redis()
        create_files()
        delete_collection_if_exists(A_COLL)
        create_resources()

    def tearDown(self):
        delete_files()
        clear_redis()
        delete_collection_if_exists(A_COLL)
        irmtrash()
        delete_resources()
        
    def do_no_event_handler(self):
        proc = Popen(["python", "irods_sync.py", "start", A, A_COLL])
        proc.wait()
        self.do_register2()
                
    def do_register(self, eh, resc_name = "demoResc"):
        proc = Popen(["python", "irods_sync.py", "start", A, A_COLL, "--event_handler", eh])
        proc.wait()
        self.do_register2(resc_name = resc_name)

    def do_register2(self, resc_name = "demoResc"):
        workers = start_workers(1)
        wait(workers)

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
                self.assertEqual(obj.replicas[0].resource_name, resc_name)
                
    def do_put(self, eh, resc_name = "demoResc", resc_root = "/var/lib/irods/Vault"):
        proc = Popen(["python", "irods_sync.py", "start", A, A_COLL, "--event_handler", eh])
        proc.wait()
        
        workers = start_workers(1)
        wait(workers)

        with iRODSSession(irods_env_file=env_file) as session:
            self.assertTrue(session.collections.exists(A_COLL))
            for i in listdir(A):
                path = join(A, i)
                rpath = A_COLL + "/" + i
                vaultpath = resc_root + "/home/rods/" + A_REMOTE + "/" + i
                self.assertTrue(session.data_objects.exists(rpath))
                a1 = read_file(path)
                
                a2 = read_data_object(session, rpath)
                self.assertEqual(a1, a2)
                
                obj = session.data_objects.get(rpath)
                self.assertEqual(obj.replicas[0].path, vaultpath)
                self.assertEqual(obj.replicas[0].resource_name, resc_name)

    def do_register_as_replica(self, eh, resc_name = "demoResc"):
        clear_redis()
        recreate_files()
        
        proc = Popen(["python", "irods_sync.py", "start", A, A_COLL, "--event_handler", eh])
        proc.wait()
        
        workers = start_workers(1)
        wait(workers)

        with iRODSSession(irods_env_file=env_file) as session:
            self.assertTrue(session.collections.exists(A_COLL))
            for i in listdir(A):
                path = join(A, i)
                rpath = A_COLL + "/" + i
                self.assertTrue(session.data_objects.exists(rpath))
                a1 = read_file(path)
                
                a2 = read_data_object(session, rpath, resc_name = resc_name)
                self.assertEqual(a1, a2)
                
                obj = session.data_objects.get(rpath)
                self.assertEqual(len(obj.replicas), 2)
                self.assertNotEqual(size(session, rpath, replica_num = 0), size(session, rpath, replica_num = 1))
                self.assertIn(realpath(path), map(lambda i : obj.replicas[i].path, range(2)))
                self.assertNotEqual(size(session, rpath, resc_name = resc_name), len(a1))

    def do_update(self, eh, resc_name = "demoResc"):
        recreate_files()
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
        recreate_files()

        proc = Popen(["python", "irods_sync.py", "start", A, A_COLL, "--event_handler", eh])
        proc.wait()
        
        workers = start_workers(1)
        wait(workers)

        with iRODSSession(irods_env_file=env_file) as session:
            for i in listdir(A):
                path = join(A, i)
                rpath = A_COLL + "/" + i
                a1 = read_file(path)
                
                a2 = read_data_object(session, rpath)
                self.assertNotEqual(a1, a2)

    def test_no_event_handler(self):
        self.do_no_event_handler()
        
    def test_register(self):
        self.do_register("examples.register")

    def test_register_with_resc_hier(self):
        self.do_register("examples.register_with_resc_hier", resc_name = REGISTER_RESC)

    def test_register_with_resc_name(self):
        self.do_register("examples.register_with_resc_name", resc_name = REGISTER_RESC)

    def test_update(self):
        self.do_register("examples.update")
        self.do_update("examples.update")
        
    def test_update_with_resc_hier(self):
        self.do_register("examples.update_with_resc_hier", resc_name = REGISTER_RESC)
        self.do_update("examples.update_with_resc_hier", resc_name = REGISTER_RESC)
                
    def test_update_with_resc_name(self):
        self.do_register("examples.update_with_resc_name", resc_name = REGISTER_RESC)
        self.do_update("examples.update_with_resc_name", resc_name = REGISTER_RESC)
                
    def test_put(self):
        self.do_put("examples.put")

    def test_put_with_resc_name(self):
        self.do_put("examples.put_with_resc_name", resc_name = PUT_RESC, resc_root = PUT_RESC_PATH)

    def test_sync(self):
        self.do_put("examples.sync")
        recreate_files()
        self.do_put("examples.sync")

    def test_sync_with_resc_name(self):
        self.do_put("examples.sync_with_resc_name", resc_name = PUT_RESC, resc_root = PUT_RESC_PATH)
        recreate_files()
        self.do_put("examples.sync_with_resc_name", resc_name = PUT_RESC, resc_root = PUT_RESC_PATH)

    def test_register_as_replica_with_resc_name(self):
        self.do_put("examples.put")
        self.do_register_as_replica("examples.replica_with_resc_name", resc_name = REGISTER_RESC)

    def test_register_as_replica_with_resc_hier(self):
        self.do_put("examples.put")
        self.do_register_as_replica("examples.replica_with_resc_hier", resc_name = REGISTER_RESC)

    def test_no_sync(self):
        self.do_put("examples.no_sync")
        self.do_no_sync("examples.no_sync")
        
if __name__ == '__main__':
        unittest.main()
