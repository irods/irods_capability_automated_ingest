from .sync_task import start_synchronization, stop_synchronization, list_synchronization, monitor_synchronization
import argparse
from uuid import uuid1
import json
import sys


def get_config(args):
    return {
        "log": {
            "filename": getattr(args, "log_filename", None),
            "when": getattr(args, "log_when", None),
            "interval": getattr(args, "log_interval", None),
            "level": getattr(args, "log_level", None)
        },
        "profile": {
            "filename": getattr(args, "profile_filename", None),
            "when": getattr(args, "profile_when", None),
            "interval": getattr(args, "profile_interval", None),
            "level": getattr(args, "profile_level", None)
        },
        "redis": {
            "host": args.redis_host,
            "port": args.redis_port,
            "db": args.redis_db
        }
    }


def add_arguments(parser):
    parser.add_argument('--redis_host', action="store", metavar="REDIS HOST", type=str, default="localhost", help="redis host")
    parser.add_argument('--redis_port', action="store", metavar="REDIS PORT", type=int, default=6379, help="redis port")
    parser.add_argument('--redis_db', action="store", metavar="REDIS DB", type=int, default=0, help="redis db")
    parser.add_argument('--log_filename', action="store", metavar="LOG FILE", type=str, default=None, help="log filename")
    parser.add_argument('--log_when', action="store", metavar="LOG WHEN", type=str, default=None, help="log when")
    parser.add_argument('--log_interval', action="store", metavar="LOG INTERVAL", type=int, default=None,
                        help="log interval")
    parser.add_argument('--log_level', action="store", metavar="LOG LEVEL", type=str, default=None,
                        help="log level")
    parser.add_argument('--profile_filename', action="store", metavar="PROFILE FILE", type=str, default=None, help="profile filename")
    parser.add_argument('--profile_when', action="store", metavar="PROFILE WHEN", type=str, default=None, help="profile when")
    parser.add_argument('--profile_interval', action="store", metavar="PROFILE INTERVAL", type=int, default=None,
                        help="profile interval")
    parser.add_argument('--profile_level', action="store", metavar="PROFILE LEVEL", type=str, default=None,
                        help="profile level")


def handle_start(args):
    return start_synchronization({
        "restart_queue": args.restart_queue,
        "path_queue": args.path_queue,
        "file_queue": args.file_queue,
        "target": args.target,
        "root": args.root,
        "interval": args.interval,
        "job_name": args.job_name,
        "append_json": args.append_json,
        "ignore_cache": args.ignore_cache,
        "event_handler": args.event_handler,
        "config": get_config(args),
        "synchronous": args.synchronous,
        "progress": args.progress,
        "profile": args.profile
    })


def handle_stop(args):
    stop_synchronization(args.job_name, get_config(args))
    return 0


def handle_watch(args):
    return monitor_synchronization(args.job_name, True, get_config(args))


def handle_list(args):
    jobs = list_synchronization(get_config(args))
    print(json.dumps(jobs))
    return 0


def main():
    uuid = str(uuid1())

    parser = argparse.ArgumentParser(description='continuous synchronization utility')
    subparsers = parser.add_subparsers(help="subcommand help")

    parser_start = subparsers.add_parser("start", help="start help")
    parser_start.add_argument('root', metavar='ROOT', type=str, help='root directory')
    parser_start.add_argument('target', metavar='TARGET', type=str, help='target collection')
    parser_start.add_argument('-i', '--interval', action="store", metavar='INTERVAL', type=int, default=None, help='restart interval (in seconds)')
    parser_start.add_argument('--file_queue', action="store", metavar='FILE QUEUE', type=str, default="file", help='file queue')
    parser_start.add_argument('--path_queue', action="store", metavar='PATH QUEUE', type=str, default="path", help='path queue')
    parser_start.add_argument('--restart_queue', action="store", metavar='RESTART QUEUE', type=str, default="restart", help='restart queue')
    parser_start.add_argument('--event_handler', action="store", metavar='EVENT HANDLER', type=str, default=None, help='event handler')
    parser_start.add_argument('--job_name', action="store", metavar='JOB NAME', type=str, default=uuid, help='job name')
    parser_start.add_argument('--append_json', action="store", metavar='APPEND JSON', type=json.loads, default=None, help='append json')
    parser_start.add_argument("--ignore_cache", action="store_true", default=False, help='ignore cache')
    parser_start.add_argument('--synchronous', action="store_true", default=False, help='synchronous')
    parser_start.add_argument('--progress', action="store_true", default=False, help='progress')
    parser_start.add_argument('--profile', action="store_true", default=False, help='profile')
    add_arguments(parser_start)

    parser_start.set_defaults(func=handle_start)

    parser_stop = subparsers.add_parser("stop", help="stop help")
    parser_stop.add_argument('job_name', action="store", metavar='JOB NAME', type=str, help='job name')
    add_arguments(parser_stop)
    parser_stop.set_defaults(func=handle_stop)

    parser_watch = subparsers.add_parser("watch", help="watch help")
    parser_watch.add_argument('job_name', action="store", metavar='JOB NAME', type=str, help='job name')
    add_arguments(parser_watch)
    parser_watch.set_defaults(func=handle_watch)

    parser_list = subparsers.add_parser("list", help="list help")
    add_arguments(parser_list)
    parser_list.set_defaults(func=handle_list)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
