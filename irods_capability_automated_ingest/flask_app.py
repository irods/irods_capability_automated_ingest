from irods_capability_automated_ingest.sync_task import start_synchronization, stop_synchronization, list_synchronization
from uuid import uuid1
from flask import Flask, request
import flask
from flask_restful import reqparse, Resource, Api
import click

import os
import traceback

app = Flask(__name__)

api = Api(app)

parser_start = reqparse.RequestParser()

parser_start.add_argument('source', required=True, type=str, help='source directory')
parser_start.add_argument('target', required=True, type=str, help='target collection')
parser_start.add_argument('interval', type=int, default=None, help='restart interval (in seconds)')
parser_start.add_argument('file_queue', type=str, default="file", help='file queue')
parser_start.add_argument('path_queue', type=str, default="path", help='path queue')
parser_start.add_argument('restart_queue', type=str, default="restart", help='restart queue')
parser_start.add_argument('event_handler', type=str, default=None, help='event handler')

def put(job_name, data):
    event_handler_path = app.config.get("event_handler_path")
            
    args = parser_start.parse_args(strict=True)
    try:
        start_synchronization(args["restart_queue"], args["path_queue"], args["file_queue"], args["target"], args["source"], args["interval"], job_name, args["event_handler"], event_handler_path, data.decode("utf-8"), get_config())
        return job_name, 201
    except Exception as e:
        traceback.print_exc()
        return str(e), 400

class Jobs(Resource):
    def get(self):
        jobs = list_synchronization(get_config())
        return list(jobs)

    def put(self):
        job_name = str(uuid1())
        return put(job_name, request.data)

class Job(Resource):
    def put(self, job_name):
        return put(job_name, request.data)

    def delete(self, job_name):
        try:
            stop_synchronization(job_name, get_config())
            return "", 204
        except Exception as e:
            return str(e), 400


api.add_resource(Jobs, "/job")
api.add_resource(Job, "/job/<job_name>")


def get_config():
    return {
        "log": {
            "filename": os.environ.get("log_filename"),
            "when": os.environ.get("log_when"),
            "interval": os.environ.get("log_interval"),
            "level": os.environ.get("log_level")
        },
        "redis":{
            "host" : os.environ.get("redis_host", "localhost"),
            "port" : os.environ.get("redis_port", 6379),
            "db" : os.environ.get("redis_db", 0)
        }
    }


DEFAULT_EVENT_HANDLER_PATH = "/tmp"


builtin_run_command = flask.cli.run_command


@app.cli.command('run_app', help=builtin_run_command.help, short_help=builtin_run_command.short_help)
@click.option("--event_handler_path", default=DEFAULT_EVENT_HANDLER_PATH)
@click.pass_context
def run_app(ctx, event_handler_path, **kwargs):
    app.config["event_handler_path"] = event_handler_path
    ctx.params.pop("event_handler_path", None)
    ctx.forward(builtin_run_command)

run_app.params[:0] = builtin_run_command.params


