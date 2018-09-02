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

    ex_file_arg = args.exclude_file_type
    if ex_file_arg != None:
        ex_arg_list = [x.strip() for x in ex_file_arg[0].split(',')]

    data = {}
    data["restart_queue"] = args.restart_queue
    data["path_queue"] = args.path_queue
    data["file_queue"] = args.file_queue
    data["target"] = args.target
    data["root"] = args.root
    data["interval"] = args.interval
    data["job_name"] = args.job_name
    data["append_json"] = args.append_json
    data["ignore_cache"] = args.ignore_cache
    data["initial_ingest"] = args.initial_ingest
    data["event_handler"] = args.event_handler
    data["config"] = get_config(args)
    data["synchronous"] = args.synchronous
    data["progress"] = args.progress
    data["profile"] = args.profile
    data["list_dir"] = args.list_dir
    data["scan_dir_list"] = args.scan_dir_list
    data["exclude_file_type"] = ex_arg_list
    data['exclude_file_name'] = [ ''.join(r) for r in args.exclude_file_name ]
    data['exclude_directory_name'] = [ ''.join(r) for r in args.exclude_directory_name ]
    data['idle_disconnect_seconds'] = args.irods_idle_disconnect_seconds

    return start_synchronization(data)

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
    parser_start.add_argument('root', metavar='SOURCE_DIRECTORY', type=str, help='source_directory')
    parser_start.add_argument('target', metavar='TARGET_COLLECTION', type=str, help='target_collection')
    parser_start.add_argument('-i', '--interval', action="store", metavar='INTERVAL', type=int, default=None, help='restart interval (in seconds)')
    parser_start.add_argument('--file_queue', action="store", metavar='FILE QUEUE', type=str, default="file", help='file queue')
    parser_start.add_argument('--path_queue', action="store", metavar='PATH QUEUE', type=str, default="path", help='path queue')
    parser_start.add_argument('--restart_queue', action="store", metavar='RESTART QUEUE', type=str, default="restart", help='restart queue')
    parser_start.add_argument('--event_handler', action="store", metavar='EVENT HANDLER', type=str, default=None, help='event handler')
    parser_start.add_argument('--job_name', action="store", metavar='JOB NAME', type=str, default=uuid, help='job name')
    parser_start.add_argument('--append_json', action="store", metavar='APPEND JSON', type=json.loads, default=None, help='append json')
    parser_start.add_argument("--ignore_cache", action="store_true", default=False, help='ignore cache')
    parser_start.add_argument("--initial_ingest", action="store_true", default=False, help='initial ingest')
    parser_start.add_argument('--synchronous', action="store_true", default=False, help='synchronous')
    parser_start.add_argument('--progress', action="store_true", default=False, help='progress')
    parser_start.add_argument('--profile', action="store_true", default=False, help='profile')
    parser_start.add_argument('--list_dir', action="store_true", default=False, help='list dir')
    parser_start.add_argument('--scan_dir_list', action="store_true", default=False, help='scan dir list')
    parser_start.add_argument('--exclude_file_type', nargs=1, action="store", default='none', help='types of files to exclude: regular, directory, character, block, socket, pipe, link')
    parser_start.add_argument('--exclude_file_name', type=list, nargs='+', action="store", default='none', help='a list of space-separated python regular expressions defining the file names to exclude such as "(\S+)exclude" "(\S+)\.hidden"')
    parser_start.add_argument('--exclude_directory_name', type=list, nargs='+', action="store", default='none', help='a list of space-separated python regular expressions defining the directory names to exclude such as "(\S+)exclude" "(\S+)\.hidden"')
    parser_start.add_argument('--irods_idle_disconnect_seconds', action="store", metavar='DISCONNECT IN SECONDS', type=int, default=None, help='irods disconnect time in seconds')
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
