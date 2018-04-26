from irods_capability_automated_ingest.sync_task import start_synchronization, stop_synchronization, list_synchronization
from uuid import uuid1
from flask import Flask
from flask_restful import reqparse, Resource, Api

import os

app = Flask(__name__)

api = Api(app)

def add_arguments(parser):
    parser.add_argument('redis_host', type=str, default="localhost", help="redis host")
    parser.add_argument('redis_port', type=int, default=6379, help="redis port")
    parser.add_argument('redis_db', type=int, default=0, help="redis db")
    parser.add_argument('log_filename', type=str, default=None, help="log filename")
    parser.add_argument('log_when', type=str, default=None, help="log when")
    parser.add_argument('log_interval', type=int, default=None, help="log interval")
    parser.add_argument('log_level', type=str, default=None, help="log level")

parser_start = reqparse.RequestParser()

parser_start.add_argument('source', required=True, type=str, help='source directory')
parser_start.add_argument('target', required=True, type=str, help='target collection')
parser_start.add_argument('interval', type=int, default=None, help='restart interval (in seconds)')
parser_start.add_argument('file_queue', type=str, default="file", help='file queue')
parser_start.add_argument('path_queue', type=str, default="path", help='path queue')
parser_start.add_argument('restart_queue', type=str, default="restart", help='restart queue')
parser_start.add_argument('event_handler', type=str, default=None, help='event handler')

add_arguments(parser_start)

def put(job_name):
    args = parser_start.parse_args(strict=True)
    try:
        start_synchronization(args["restart_queue"], args["path_queue"], args["file_queue"], args["target"], args["source"], args["interval"], job_name, args["event_handler"], get_config())
        return job_name, 201
    except Exception as e:
        return str(e), 400

class Jobs(Resource):
    def get(self):
        jobs = list_synchronization(get_config())
        return list(jobs)

    def put(self):
        job_name = str(uuid1())
        return put(job_name)
        
class Job(Resource):
    def put(self, job_name):
        return put(job_name)
    
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

if __name__ == "__main__":
    app.run()
