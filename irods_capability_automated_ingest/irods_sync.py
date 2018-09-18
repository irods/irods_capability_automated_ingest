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
    parser.add_argument('--log_filename', action="store", type=str, default=None, help="Specify name of log file.")
    parser.add_argument('--log_when', action="store", type=str, default=None, help="Specify the type of log_interval (see TimedRotatingFileHandler).")
    parser.add_argument('--log_interval', action="store", type=int, default=None, help="Specify the interval with which to rollover the ingest log file.")
    parser.add_argument('--log_level', action="store", type=str, default=None, help="Specify minimum level of message to log (DEBUG, INFO, WARNING, ERROR).")
    parser.add_argument('--profile_filename', action="store", type=str, default=None, help="Specify name of profile filename.")
    parser.add_argument('--profile_when', action="store", type=str, default=None, help="Specify the type of profile_interval (see TimedRotatingFileHandler).")
    parser.add_argument('--profile_interval', action="store", type=int, default=None, help="Specify the interval with which to rollover the ingest profile log file.")
    parser.add_argument('--profile_level', action="store", type=str, default=None, help="Specify minimum level of message to log for profiling (DEBUG, INFO, WARNING, ERROR).")
    parser.add_argument('--redis_host', action="store", type=str, default="localhost", help="Domain or IP address of Redis host.")
    parser.add_argument('--redis_port', action="store", type=int, default=6379, help="Port number for Redis.")
    parser.add_argument('--redis_db', action="store", type=int, default=0, help="Redis DB number to use for ingest.")


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
    data["job_name"] = args.job_name if args.job_name else str(uuid1())
    data["append_json"] = args.append_json
    data["ignore_cache"] = args.ignore_cache
    data["initial_ingest"] = args.initial_ingest
    data["event_handler"] = args.event_handler
    data["config"] = get_config(args)
    data["synchronous"] = args.synchronous
    data["progress"] = args.progress
    data["profile"] = args.profile
    data["files_per_task"] = args.files_per_task
    data["s3_endpoint_domain"] = args.s3_endpoint_domain
    data["s3_region_name"] = args.s3_region_name
    data["s3_keypair"] = args.s3_keypair
    data["s3_proxy_url"] = args.s3_proxy_url
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
    parser = argparse.ArgumentParser(description='continuous synchronization utility')
    subparsers = parser.add_subparsers(help="subcommand help")

    parser_start = subparsers.add_parser("start", formatter_class=argparse.ArgumentDefaultsHelpFormatter, help="start help")
    parser_start.add_argument('root', metavar='SOURCE_DIRECTORY', type=str, help='Source directory or S3 folder to scan.')
    parser_start.add_argument('target', metavar='TARGET_COLLECTION', type=str, help='Target iRODS collection for data objects (created if non-existent).')
    parser_start.add_argument('-i', '--interval', action="store", type=int, default=None, help='Restart interval (in seconds). If absent, will only sync once.')
    parser_start.add_argument('--file_queue', action="store", type=str, default="file", help='Name for the file queue.')
    parser_start.add_argument('--path_queue', action="store", type=str, default="path", help='Name for the path queue.')
    parser_start.add_argument('--restart_queue', action="store", type=str, default="restart", help='Name for the restart queue.')
    parser_start.add_argument('--event_handler', action="store", type=str, default=None, help='Path to event handler file')
    parser_start.add_argument('--job_name', action="store", type=str, default=None, help='Reference name for ingest job (defaults to generated uuid)')
    parser_start.add_argument('--append_json', action="store", type=json.loads, default=None, help='Append json output')
    parser_start.add_argument("--ignore_cache", action="store_true", default=False, help='Ignore last sync time in cache - like starting a new sync')
    parser_start.add_argument("--initial_ingest", action="store_true", default=False, help='Use this flag on initial ingest to avoid check for data object paths already in iRODS.')
    parser_start.add_argument('--synchronous', action="store_true", default=False, help='Block until sync job is completed.')
    parser_start.add_argument('--progress', action="store_true", default=False, help='Show progress bar and task counts (must have --synchronous flag).')
    parser_start.add_argument('--profile', action="store_true", default=False, help='Generate JSON file of system activity profile during ingest.')
    parser_start.add_argument('--files_per_task', action="store", type=int, default='50', help='Number of paths to process in a given task on the queue.')
    parser_start.add_argument('--s3_endpoint_domain', action="store", type=str, default='s3.amazonaws.com', help='S3 endpoint domain')
    parser_start.add_argument('--s3_region_name', action="store", type=str, default='us-east-1', help='S3 region name')
    parser_start.add_argument('--s3_keypair', action="store", type=str, default=None, help='Path to S3 keypair file')
    parser_start.add_argument('--s3_proxy_url', action="store", type=str, default=None, help='URL to proxy for S3 access')
    parser_start.add_argument('--exclude_file_type', nargs=1, action="store", default='none', help='types of files to exclude: regular, directory, character, block, socket, pipe, link')
    parser_start.add_argument('--exclude_file_name', type=list, nargs='+', action="store", default='none', help='a list of space-separated python regular expressions defining the file names to exclude such as "(\S+)exclude" "(\S+)\.hidden"')
    parser_start.add_argument('--exclude_directory_name', type=list, nargs='+', action="store", default='none', help='a list of space-separated python regular expressions defining the directory names to exclude such as "(\S+)exclude" "(\S+)\.hidden"')
    parser_start.add_argument('--irods_idle_disconnect_seconds', action="store", type=int, default=60, help='irods disconnect time in seconds')
    add_arguments(parser_start)

    parser_start.set_defaults(func=handle_start)

    parser_stop = subparsers.add_parser("stop", formatter_class=argparse.ArgumentDefaultsHelpFormatter, help="stop help")
    parser_stop.add_argument('job_name', action="store", type=str, help='job name')
    add_arguments(parser_stop)
    parser_stop.set_defaults(func=handle_stop)

    parser_watch = subparsers.add_parser("watch", formatter_class=argparse.ArgumentDefaultsHelpFormatter, help="watch help")
    parser_watch.add_argument('job_name', action="store", type=str, help='job name')
    add_arguments(parser_watch)
    parser_watch.set_defaults(func=handle_watch)

    parser_list = subparsers.add_parser("list", formatter_class=argparse.ArgumentDefaultsHelpFormatter, help="list help")
    add_arguments(parser_list)
    parser_list.set_defaults(func=handle_list)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
