import argparse
from rq import Connection, Worker
from redis import StrictRedis
from rq.handlers import move_to_failed_queue
from irods_capability_automated_ingest.sync_utils import retry_handler

parser = argparse.ArgumentParser(description='continuous synchronization utility')

parser.add_argument('--file_queue', metavar='FILE QUEUE', type=str, default="file", help='file queue')
parser.add_argument('--path_queue', metavar='PATH QUEUE', type=str, default="path", help='path queue')
parser.add_argument('--restart_queue', metavar='RESTART QUEUE', type=str, default="restart", help='restart queue')
parser.add_argument('--burst', action="store_true", default=False, help='burst')

parser.add_argument('-u', '--url', action="store", metavar='URL', type=str, default=None, help='url')
args = parser.parse_args()

if args.url is not None:
    r = StrictRedis.from_url(args.url)
else:
    r = StrictRedis()

c = Connection(r)

with Connection(r):
    qs = [args.file_queue, args.path_queue, args.restart_queue]

    w = Worker(qs, exception_handlers=[retry_handler, move_to_failed_queue])
    w.work(burst=args.burst)
