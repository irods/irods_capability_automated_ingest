from irods_capability_automated_ingest.sync_task import start_synchronization, stop_synchronization, list_synchronization
from uuid import uuid1
from flask import Flask, request
import flask
from flask_restful import reqparse, Resource, Api
import click

import os
import traceback
import yaml

app = Flask(__name__)

api = Api(app)

parser_start = reqparse.RequestParser()


def put(job_name, data):
    data2 = yaml.load(data.decode("utf-8"))

    data2["event_handler_path"] = app.config.get("event_handler_path")
    data2.setdefault('max_retries', 0)
    data2.setdefault('file_queue', "file")
    data2.setdefault('path_queue', "path")
    data2.setdefault('restart_queue', "restart")
    data2.setdefault("ignore_cache", False)
    data2.setdefault("initial_ingest", False)
    data2.setdefault("synchronous", False)
    data2.setdefault("timeout", 3600)
    data2.setdefault("interval", None)
    data2.setdefault("profile", False)
    data2.setdefault("files_per_task", 50)
    data2["job_name"] = job_name
    data2["config"] = get_config()

    try:
        start_synchronization(data2)
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
        "redis": {
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


