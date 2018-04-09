from irods_capability_automated_ingest.sync_task import start_synchronization, stop_synchronization, list_synchronization
import argparse
from uuid import uuid1

def get_logging_config(args):
    return {
        "filename": getattr(args, "log_filename", None),
        "when": getattr(args, "log_when", None),
        "interval": getattr(args, "log_interval", None),
        "level" : getattr(args, "log_level", None)
    }

def add_logging_arguments(parser):
    parser.add_argument('--log_filename', action="store", metavar="LOG FILE", type=str, default=None, help="log filename")
    parser.add_argument('--log_when', action="store", metavar="LOG WHEN", type=str, default=None, help="log when")
    parser.add_argument('--log_interval', action="store", metavar="LOG INTERVAL", type=int, default=None,
                        help="log interval")
    parser.add_argument('--log_level', action="store", metavar="LOG LEVEL", type=str, default=None,
                        help="log level")


def handle_start(args):
    start_synchronization(args.restart_queue, args.path_queue, args.file_queue, args.target, args.root, args.interval, args.job_name, args.event_handler, get_logging_config(args))

def handle_stop(args):
    stop_synchronization(args.job_name, get_logging_config(args))

def handle_list(args):
    list_synchronization(get_logging_config(args))

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
    add_logging_arguments(parser_start)


    parser_start.set_defaults(func=handle_start)

    parser_stop = subparsers.add_parser("stop", help="stop help")
    parser_stop.add_argument('job_name', action="store", metavar='JOB NAME', type=str, help='job name')
    add_logging_arguments(parser_stop)
    parser_stop.set_defaults(func=handle_stop)

    parser_list = subparsers.add_parser("list", help="list help")
    add_logging_arguments(parser_list)
    parser_list.set_defaults(func=handle_list)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()