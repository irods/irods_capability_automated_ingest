from redis import Redis
from rq import Queue
from sync_task import start_synchronization, stop_synchronization
import argparse
from datetime import timedelta
import logging
import sys

def handle_start(args):
    start_synchronization(args.restart_queue, args.path_queue, args.file_queue, args.target, args.root, args.interval, args.event_handler)

def handle_stop(args):
    stop_synchronization()

parser = argparse.ArgumentParser(description='continuous synchronization utility')
subparsers = parser.add_subparsers(help="subcommand help")

parser_start = subparsers.add_parser("start", help="start help")
parser_start.add_argument('root', metavar='ROOT', type=str, help='root directory')
parser_start.add_argument('target', metavar='TARGET', type=str, help='target collection')
parser_start.add_argument('-i', '--interval', action="store", metavar='INTERVAL', type=int, required=True, help='restart interval (in seconds)')
parser_start.add_argument('--file_queue', action="store", metavar='FILE QUEUE', type=str, default="file", help='file queue')
parser_start.add_argument('--path_queue', action="store", metavar='PATH QUEUE', type=str, default="path", help='path queue')
parser_start.add_argument('--restart_queue', action="store", metavar='RESTART QUEUE', type=str, default="restart", help='restart queue')
parser_start.add_argument('--event_handler', action="store", metavar='EVENT HANDLER', type=str, default=None, help='event handler')

parser_start.set_defaults(func=handle_start)

parser_stop = subparsers.add_parser("stop", help="stop help")
parser_stop.set_defaults(func=handle_stop)

args = parser.parse_args()
args.func(args)

