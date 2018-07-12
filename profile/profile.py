import sys
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import argparse

parser = argparse.ArgumentParser(description='Ingest profile data into Elasticsearch')
parser.add_argument('input_file', metavar='INPUT FILE', type=str,
                    help='input file')
parser.add_argument('--elasticsearch_host', metavar='ELASTICSEARCH HOST', type=str, default="localhost",
                    help='elasticsearch host')
parser.add_argument('elasticsearch_index', metavar='ELASTICSEARCH INDEX', type=str,
                    help='elasticsearch index')
parser.add_argument('--additional_key', dest='keys', action='store', nargs="*", default=[],
                    help='additional key')

args = parser.parse_args()

input_file = args.input_file
keys = args.keys
output = args.elasticsearch_host
index = args.elasticsearch_index

es = Elasticsearch(output)


def task_action():

    task_buf = {}

    i = 0
    with open(input_file, "r") as f:
    
        line = f.readline().rstrip("\n")
        while line != "":
            obj = json.loads(line)

            event_id = obj["event_id"]
            # print(obj)
            buf = task_buf.get(event_id)
            if buf is None:
                task_buf[event_id] = obj
            else:
                del task_buf[event_id]
                if obj["event"] == "task_prerun":
                    start = obj["@timestamp"]
                    finish = buf["@timestamp"]
                else:
                    start = buf["@timestamp"]
                    finish = obj["@timestamp"]

                di = {
                    "start": start,
                    "finish": finish,
                    "hostname": obj["hostname"],
                    "index": obj["index"],
                    "event_name": obj["event_name"],
                    "event_id": obj["event_id"]
                }

                for key in keys:
                    di[key] = obj[key]

                d = {
                    "_index": index,
                    "_type": "document",
                    "_source": di
                }
                i += 1
                print(i)
                yield d
            line = f.readline().rstrip("\n")


bulk(es, task_action())
