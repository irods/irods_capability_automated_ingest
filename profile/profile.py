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

try:
    es.indices.create(index, body={
        "mappings": {
            "document": {
                "properties": {
                    "hostname": {
                        "type": "keyword"
                    }
                }
            }
        }
    })
except Exception as e:
    print(e)
    
def task_action():

    task_buf = {}
    task_counter = {}

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

                event_name = obj["event_name"]
                di = {
                    "start": start,
                    "finish": finish,
                    "hostname": obj["hostname"],
                    "index": obj["index"],
                    "event_name": event_name,
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
                if event_name in task_counter:
                    task_counter[event_name] += 1
                else:
                    task_counter[event_name] = 1
                yield d
            line = f.readline().rstrip("\n")
    if len(task_buf) != 0:
        print(task_buf)

    print(task_counter)


bulk(es, task_action())
