import unittest
import os
import os.path
from unittest import TestCase
from redis import StrictRedis
from subprocess import Popen, DEVNULL, PIPE
from signal import SIGINT
from os import makedirs, listdir, environ
from shutil import rmtree
from os.path import join, realpath
from irods.session import iRODSSession
from tempfile import NamedTemporaryFile

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

A = "a"
A_REMOTE = "a_remote"
A_COLL = "/tempZone/home/rods/" + A_REMOTE

def create_files():
    makedirs(A)
    for i in range(10):
        with open(join(A,str(i)), "w") as f:
            f.write("i" * i)

def delete_files():
    rmtree(A)

def read_file(path):
    with open(path) as f:
        return f.read()

def read_data_object(session, path):
    with NamedTemporaryFile() as tf:
        session.data_objects.get(path, file=tf.name, forceFlag="")
        return read_file(tf.name)
    
def delete_collection(coll):
    with iRODSSession(irods_env_file=env_file) as session:
        session.collections.remove(coll)

def delete_collection_if_exists(coll):
    with iRODSSession(irods_env_file=env_file) as session:
        if(session.collections.exists(coll)):
           session.collections.remove(coll)
           
try:
    env_file = os.environ['IRODS_ENVIRONMENT_FILE']
except KeyError:
    env_file = os.path.expanduser('~/.irods/irods_environment.json')

class TestRegister(TestCase):
    def setUp(self):
        clear_redis()
        create_files()
        delete_collection_if_exists(A_COLL)

    def tearDown(self):
        delete_files()
        clear_redis()
        delete_collection_if_exists(A_COLL)

    def test_register(self):

        proc = Popen(["python", "irods_sync.py", "start", A, A_COLL, "--event_handler", "examples.register"])
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
                
                a2 = read_data_object(session, rpath)
                self.assertEqual(a1, a2)
                
                obj = session.data_objects.get(rpath)
                self.assertEqual(obj.replicas[0].path, realpath(path))

        
if __name__ == '__main__':
        unittest.main()
