
from threading import Thread, Lock
from multiprocessing import Queue
from irods.session import iRODSSession

import time
import os

class irods_connection_pool(Thread):
    pool_size = 0
    done_flag = False
    done_mutex = Lock()
    kwargs = {}

    def instantiate_new_connections(self, num):
        self.log.debug('connection_pool - making '+str(num)+' new connections')
        for i in range(num):
            self.connections.put(iRODSSession(**self.kwargs))

    def __init__(self, logger, kwargs, size=2):
        super().__init__(group=None, target=None, name=None, daemon=None)
        self.log = logger
        self.kwargs = kwargs
        self.pool_size = size
        self.connections = Queue(maxsize=size)
        self.instantiate_new_connections(self.pool_size)
        self.start()

    def cleanup(self):
        while not self.connections.empty():
            e = self.connections.get()
            e.cleanup()

    def get(self):
        self.log.debug('connection_pool - fetching a connection')
        val = self.connections.get()
        return val

    def run(self):
        exit_flag = False
        while not exit_flag:
            conn_needed = self.pool_size - self.connections.qsize()
            if conn_needed > 0:
                self.instantiate_new_connections(conn_needed)

            with self.done_mutex:
                exit_flag = self.done_flag

            time.sleep(0.1)

    def end(self):
        self.log.debug('connection_pool - exiting')
        with self.done_mutex:
            self.done_flag = True
        self.join()
        self.cleanup()

    def __del__(self):
        self.end()


'''
# =-=-=-=-=-=-=-
connection_pool_singleton = {}

def init_connection_pool(kwargs, size=2):
    if None == connection_pool_singleton:
        connection_pool_singleton = irods_connection_pool(kwargs, size)
        connection_pool_singleton.start()

def get_connection_pool():
    return connection_pool_singleton
'''
