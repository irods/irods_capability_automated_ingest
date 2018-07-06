import sys
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

input = sys.argv[1]
output = sys.argv[2]
es = Elasticsearch(output)

def task_action():
    
    task_buf = {}

    i = 0
    with open(input, "r") as f:
    
        line = f.readline().rstrip("\n")
        while line != "":
            obj = json.loads(line)

            task_id = obj["task_id"]
            print(obj)
            buf = task_buf.get(task_id)
            if buf is None:
                task_buf[task_id] = obj
            else:
                del task_buf[task_id]
                if obj["event"] == "task_prerun":
                    start=obj["@timestamp"]
                    finish=buf["@timestamp"]
                else:
                    start=buf["@timestamp"]
                    finish=obj["@timestamp"]
                d = {
                    "_index": "icaiprofile",
                    "_type": "document",
#                    "_id" : i,
#                    "_routing": 5,
#                    "pipeline": "icaipipeline",
                    "_source": {
                        "hostname": obj["hostname"],
                        "index": obj["index"],
                        "start": start,
                        "finish": finish,
                        "task_name": obj["task_name"],
                        "task_id": obj["task_id"]
                    }
                }
                i += 1
                yield d
            line = f.readline().rstrip("\n")
        

bulk(es, task_action())
