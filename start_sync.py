from redis import Redis
from rq import Queue
from sync_task import start_synchronization
import argparse
from datetime import timedelta
import logging
import sys

parser = argparse.ArgumentParser(description='continuous synchronization utility')
parser.add_argument('root', metavar='ROOT', type=str, help='root directory')
parser.add_argument('target', metavar='TARGET', type=str, help='target collection')
parser.add_argument('-i', '--interval', action="store", metavar='INTERVAL', type=int, required=True, help='restart interval (in seconds)')
parser.add_argument('--file_queue', action="store", metavar='FILE QUEUE', type=str, default="file", help='file queue')
parser.add_argument('--path_queue', action="store", metavar='PATH QUEUE', type=str, default="path", help='path queue')
parser.add_argument('--restart_queue', action="store", metavar='RESTART QUEUE', type=str, default="restart", help='restart queue')

args = parser.parse_args()

start_synchronization(args.restart_queue, args.path_queue, args.file_queue, args.target, args.root, timedelta(seconds=args.interval))
