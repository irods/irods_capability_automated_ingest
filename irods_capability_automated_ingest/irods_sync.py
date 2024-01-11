from uuid import uuid1
from . import sync_actions
import argparse
import json
import sys


def get_config(args):
    return {
        "log": {
            "filename": getattr(args, "log_filename", None),
            "when": getattr(args, "log_when", None),
            "interval": getattr(args, "log_interval", None),
            "level": getattr(args, "log_level", None),
        },
        "profile": {
            "filename": getattr(args, "profile_filename", None),
            "when": getattr(args, "profile_when", None),
            "interval": getattr(args, "profile_interval", None),
            "level": getattr(args, "profile_level", None),
        },
        "redis": {
            "host": args.redis_host,
            "port": args.redis_port,
            "db": args.redis_db,
        },
    }


def get_celery_broker_info():
    from os import environ

    env_url = environ["CELERY_BROKER_URL"]
    if env_url is None:
        host = "localhost"
        port = 6379
        db = 0
    else:
        url = env_url.split("://")[1].split(":")
        host = url[0]
        port = url[1].split("/")[0]
        db = url[1].split("/")[1]

    return host, port, db


class character_map_argument_error(Exception):
    pass


# Make sure, if a character_map method is defined for the given event handler, that it
# returns a dictionary (or argument for construction of dictionary) appropriate within the
# conventions laid out in the README.  Also, within reason, check any characters explicitly
# named for remapping. To satisfy the principle of least surprise, they should at least
# be restricted to being strings of length one.


def check_event_handler(filename):
    namespace = {}
    if filename is not None:
        exec(open(filename, "r").read(), namespace, namespace)
        ev_hdlr_class = namespace["event_handler"]
        char_map_method = getattr(ev_hdlr_class, "character_map", None)
        error_message = ""
        if char_map_method:
            returned = char_map_method()
            try:
                char_mapper = dict(returned)
            except TypeError:
                error_message = "character_map() method must return a dict or iterable of key value tuples"
                raise character_map_argument_error(error_message)
            for key, value in char_mapper.items():
                if (
                    isinstance(key, str)
                    and len(key) > 1
                    or isinstance(key, tuple)
                    and any(len(s) > 1 for s in key)
                    or isinstance(value, str)
                    and len(value) > 1
                ):
                    error_message = "character_map()'s returned object should denote only single-character substitutions"
                    raise character_map_argument_error(error_message)


def add_arguments(parser):
    host, port, db = get_celery_broker_info()

    parser.add_argument(
        "--log_filename",
        action="store",
        type=str,
        default=None,
        help="Specify name of log file.",
    )
    parser.add_argument(
        "--log_when",
        action="store",
        type=str,
        default=None,
        help="Specify the type of log_interval (see TimedRotatingFileHandler).",
    )
    parser.add_argument(
        "--log_interval",
        action="store",
        type=int,
        default=None,
        help="Specify the interval with which to rollover the ingest log file.",
    )
    parser.add_argument(
        "--log_level",
        action="store",
        type=str,
        default=None,
        help="Specify minimum level of message to log (DEBUG, INFO, WARNING, ERROR).",
    )
    parser.add_argument(
        "--profile_filename",
        action="store",
        type=str,
        default=None,
        help="Specify name of profile filename.",
    )
    parser.add_argument(
        "--profile_when",
        action="store",
        type=str,
        default=None,
        help="Specify the type of profile_interval (see TimedRotatingFileHandler).",
    )
    parser.add_argument(
        "--profile_interval",
        action="store",
        type=int,
        default=None,
        help="Specify the interval with which to rollover the ingest profile log file.",
    )
    parser.add_argument(
        "--profile_level",
        action="store",
        type=str,
        default=None,
        help="Specify minimum level of message to log for profiling (DEBUG, INFO, WARNING, ERROR).",
    )
    parser.add_argument(
        "--redis_host",
        action="store",
        type=str,
        default=host,
        help="Domain or IP address of Redis host.",
    )
    parser.add_argument(
        "--redis_port",
        action="store",
        type=int,
        default=port,
        help="Port number for Redis.",
    )
    parser.add_argument(
        "--redis_db",
        action="store",
        type=int,
        default=db,
        help="Redis DB number to use for ingest.",
    )


def handle_start(args):
    ex_file_arg = args.exclude_file_type
    if ex_file_arg != None:
        ex_arg_list = [x.strip() for x in ex_file_arg[0].split(",")]

    check_event_handler(args.event_handler)

    data = {}
    data["restart_queue"] = args.restart_queue
    data["path_queue"] = args.path_queue
    data["file_queue"] = args.file_queue
    data["target"] = args.target
    data["src_path"] = args.src_path
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
    data["s3_secure_connection"] = not args.s3_insecure_connection
    data["s3_multipart_chunksize_in_mib"] = args.s3_multipart_chunksize_in_mib
    data["exclude_file_type"] = ex_arg_list
    data["exclude_file_name"] = ["".join(r) for r in args.exclude_file_name]
    data["exclude_directory_name"] = ["".join(r) for r in args.exclude_directory_name]
    data["idle_disconnect_seconds"] = args.irods_idle_disconnect_seconds

    return sync_actions.start_job(data)


def handle_stop(args):
    sync_actions.stop_job(args.job_name, get_config(args))
    return 0


def handle_watch(args):
    return sync_actions.monitor_job(args.job_name, True, get_config(args))


def handle_list(args):
    jobs = sync_actions.list_jobs(get_config(args))
    print(json.dumps(jobs))
    return 0


def main():
    parser = argparse.ArgumentParser(description="continuous synchronization utility")
    subparsers = parser.add_subparsers(help="subcommand help")

    parser_start = subparsers.add_parser(
        "start",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="start help",
    )
    parser_start.add_argument(
        "src_path",
        metavar="SOURCE_DIRECTORY",
        type=str,
        help="Source directory or S3 folder to scan.",
    )
    parser_start.add_argument(
        "target",
        metavar="TARGET_COLLECTION",
        type=str,
        help="Target iRODS collection for data objects (created if non-existent).",
    )
    parser_start.add_argument(
        "-i",
        "--interval",
        action="store",
        type=int,
        default=None,
        help="Restart interval (in seconds). If absent, will only sync once.",
    )
    parser_start.add_argument(
        "--file_queue",
        action="store",
        type=str,
        default="file",
        help="Name for the file queue.",
    )
    parser_start.add_argument(
        "--path_queue",
        action="store",
        type=str,
        default="path",
        help="Name for the path queue.",
    )
    parser_start.add_argument(
        "--restart_queue",
        action="store",
        type=str,
        default="restart",
        help="Name for the restart queue.",
    )
    parser_start.add_argument(
        "--event_handler",
        action="store",
        type=str,
        default=None,
        help="Path to event handler file",
    )
    parser_start.add_argument(
        "--job_name",
        action="store",
        type=str,
        default=None,
        help="Reference name for ingest job (defaults to generated uuid)",
    )
    parser_start.add_argument(
        "--append_json",
        action="store",
        type=json.loads,
        default=None,
        help="Append json output",
    )
    parser_start.add_argument(
        "--ignore_cache",
        action="store_true",
        default=False,
        help="Ignore last sync time in cache - like starting a new sync",
    )
    parser_start.add_argument(
        "--initial_ingest",
        action="store_true",
        default=False,
        help="Use this flag on initial ingest to avoid check for data object paths already in iRODS.",
    )
    parser_start.add_argument(
        "--synchronous",
        action="store_true",
        default=False,
        help="Block until sync job is completed.",
    )
    parser_start.add_argument(
        "--progress",
        action="store_true",
        default=False,
        help="Show progress bar and task counts (must have --synchronous flag).",
    )
    parser_start.add_argument(
        "--profile",
        action="store_true",
        default=False,
        help="Generate JSON file of system activity profile during ingest.",
    )
    parser_start.add_argument(
        "--files_per_task",
        action="store",
        type=int,
        default="50",
        help="Number of paths to process in a given task on the queue.",
    )
    parser_start.add_argument(
        "--s3_endpoint_domain",
        action="store",
        type=str,
        default="s3.amazonaws.com",
        help="S3 endpoint domain",
    )
    parser_start.add_argument(
        "--s3_region_name",
        action="store",
        type=str,
        default="us-east-1",
        help="S3 region name",
    )
    parser_start.add_argument(
        "--s3_keypair",
        action="store",
        type=str,
        default=None,
        help="Path to S3 keypair file",
    )
    parser_start.add_argument(
        "--s3_proxy_url",
        action="store",
        type=str,
        default=None,
        help="URL to proxy for S3 access",
    )
    parser_start.add_argument(
        "--s3_insecure_connection",
        action="store_true",
        default=False,
        help="Do not use SSL when connecting to S3 endpoint",
    )
    parser_start.add_argument(
        "--s3_multipart_chunksize_in_mib",
        action="store",
        type=int,
        default=8,
        choices=range(5, 5001),
        metavar="[5-5000]",
        help="Chunk size in mebibytes for multipart S3 uploads. Minimum part size is 5 MiB and the maximum part size is 5000 MiB.",
    )
    parser_start.add_argument(
        "--exclude_file_type",
        nargs=1,
        action="store",
        default="none",
        help="types of files to exclude: regular, directory, character, block, socket, pipe, link",
    )
    parser_start.add_argument(
        "--exclude_file_name",
        type=list,
        nargs="+",
        action="store",
        default="none",
        help='a list of space-separated python regular expressions defining the file names to exclude such as "(\S+)exclude" "(\S+)\.hidden"',
    )
    parser_start.add_argument(
        "--exclude_directory_name",
        type=list,
        nargs="+",
        action="store",
        default="none",
        help='a list of space-separated python regular expressions defining the directory names to exclude such as "(\S+)exclude" "(\S+)\.hidden"',
    )
    parser_start.add_argument(
        "--irods_idle_disconnect_seconds",
        action="store",
        type=int,
        default=60,
        help="irods disconnect time in seconds",
    )
    add_arguments(parser_start)

    parser_start.set_defaults(func=handle_start)

    parser_stop = subparsers.add_parser(
        "stop", formatter_class=argparse.ArgumentDefaultsHelpFormatter, help="stop help"
    )
    parser_stop.add_argument("job_name", action="store", type=str, help="job name")
    add_arguments(parser_stop)
    parser_stop.set_defaults(func=handle_stop)

    parser_watch = subparsers.add_parser(
        "watch",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="watch help",
    )
    parser_watch.add_argument("job_name", action="store", type=str, help="job name")
    add_arguments(parser_watch)
    parser_watch.set_defaults(func=handle_watch)

    parser_list = subparsers.add_parser(
        "list", formatter_class=argparse.ArgumentDefaultsHelpFormatter, help="list help"
    )
    add_arguments(parser_list)
    parser_list.set_defaults(func=handle_list)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
