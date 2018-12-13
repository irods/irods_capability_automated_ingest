# iRODS Automated Ingest Framework

The automated ingest framework gives iRODS an enterprise solution that solves two major use cases: getting existing data under management and ingesting incoming data hitting a landing zone.

Based on the Python iRODS Client and Celery, this framework can scale up to match the demands of data coming off instruments, satellites, or parallel filesystems.

The example diagrams below show a filesystem scanner and a landing zone.

![Automated Ingest: Filesystem Scanner Diagram](capability_automated_ingest_filesystem_scanner.jpg)

![Automated Ingest: Landing Zone Diagram](capability_automated_ingest_landing_zone.jpg)

## Usage options

### Redis options
| option | effect | default |
| ----   |  ----- |  ----- |
| redis_host | Domain or IP address of Redis host | localhost |
| redis_port | Port number for Redis | 6379 |
| redis_db | Redis DB number to use for ingest | 0 |

### S3 options
To scan S3 bucket, minimally requires `--s3_keypair` and source path of the form `/bucket_name/path/to/root/folder`.

| option | effect | default |
| ----   |  ----- |  ----- |
| s3_keypair | path to S3 keypair file | None |
| s3_endpoint_domain | S3 endpoint domain | s3.amazonaws.com |
| s3_region_name | S3 region name | us-east-1 |
| s3_proxy_url | URL to proxy for S3 access | None |

### Logging/Profiling options
| option | effect | default |
| ----   |  ----- |  ----- |
| log_filename | Path to output file for logs | None |
| log_level | Minimum level of message to log | None |
| log_interval | Time interval with which to rollover ingest log file | None |
| log_when | Type/units of log_interval (see TimedRotatingFileHandler) | None |

`--profile` allows you to use vis to visualize a profile of Celery workers over time of ingest job.

| option | effect | default |
| ----   |  ----- |  ----- |
| profile_filename | Specify name of profile filename (JSON output) | None |
| profile_level | Minimum level of message to log for profiling | None |
| profile_interval | Time interval with which to rollover ingest profile file | None |
| profile_when | Type/units of profile_interval (see TimedRotatingFileHandler) | None |

### Ingest start options
These options are used at the "start" of an ingest job.

| option | effect | default |
| ----   |  ----- |  ----- |
| job_name | Reference name for ingest job | a generated uuid |
| interval | Restart interval (in seconds). If absent, will only sync once. | None |
| file_queue | Name for the file queue. | file |
| path_queue | Name for the path queue. | path |
| restart_queue | Name for the restart queue. | restart |
| event_handler | Path to event handler file | None (see "event_handler methods" below) |
| synchronous | Block until sync job is completed | False |
| progress | Show progress bar and task counts (must have --synchronous flag) | False |
| ignore_cache | Ignore last sync time in cache - like starting a new sync | False |

### Optimization options
| option | effect | default |
| ----   |  ----- |  ----- |
| exclude_file_type | types of files to exclude: regular, directory, character, block, socket, pipe, link | None |
| exclude_file_name | a list of space-separated python regular expressions defining the file names to exclude such as "(\S+)exclude" "(\S+)\.hidden" | None |
| exclude_directory_name | a list of space-separated python regular expressions defining the directory names to exclude such as "(\S+)exclude" "(\S+)\.hidden" | None |
| files_per_task | Number of paths to process in a given task on the queue | 50 |
| initial_ingest | Use this flag on initial ingest to avoid check for data object paths already in iRODS | False |
| irods_idle_disconnect_seconds | Seconds to hold open iRODS connection while idle | 60 |

## available `--event_handler` methods

| method |  effect  | default |
| ----   |   ----- |  ----- |
| pre_data_obj_create   |   user-defined python  |  none |
| post_data_obj_create   | user-defined python  |  none |
| pre_data_obj_modify     |   user-defined python   |  none |
| post_data_obj_modify     | user-defined python  |  none |
| pre_coll_create    |   user-defined python |  none |
| post_coll_create    |  user-defined python   |  none |
| pre_coll_modify    |   user-defined python |  none |
| post_coll_modify    |  user-defined python   |  none |
| as_user |   takes action as this iRODS user |  authenticated user |
| target_path | set mount path on the irods server which can be different from client mount path | client mount path |
| to_resource | defines  target resource request of operation |  as provided by client environment |
| operation | defines the mode of operation |  `Operation.REGISTER_SYNC` |
| max_retries | defines max number of retries on failure | 0 |
| timeout | defines seconds until job times out | 3600 |
| delay | defines seconds between retries | 0 |

Event handlers can use `logger` to write logs. See `structlog` for available logging methods and signatures.

### operation mode ###

| operation |  new files  | updated files  |
| ----   |   ----- | ----- |
| `Operation.REGISTER_SYNC` (default)   |  registers in catalog | updates size in catalog |
| `Operation.REGISTER_AS_REPLICA_SYNC`  |   registers first or additional replica | updates size in catalog |
| `Operation.PUT`  |   copies file to target vault, and registers in catalog | no action |
| `Operation.PUT_SYNC`  |   copies file to target vault, and registers in catalog | copies entire file again, and updates catalog |
| `Operation.PUT_APPEND`  |   copies file to target vault, and registers in catalog | copies only appended part of file, and updates catalog |
| `Operation.NO_OP` | no action | no action

`--event_handler` usage examples can be found [in the examples directory](irods_capability_automated_ingest/examples).

## Deployment

### Basic: manual redis, Celery, pip

Running the sync job and Celery workers requires a valid iRODS environment file for an authenticated iRODS user on each node.

#### Starting Redis Server
Install redis-server:
```
sudo yum install redis-server
```
```
sudo apt-get install redis-server
```
Or, build it yourself: https://redis.io/topics/quickstart

Start redis:
```
redis-server
```
Or, dameonized:
```
sudo service redis-server start
```
```
sudo systemctl start redis
```

The [Redis documentation](https://redis.io/topics/admin) also recommends an additional step:
> Make sure to set the Linux kernel overcommit memory setting to 1. Add vm.overcommit_memory = 1 to /etc/sysctl.conf and then reboot or run the command sysctl vm.overcommit_memory=1 for this to take effect immediately.

This allows the Linux kernel to overcommit virtual memory even if this exceeds the physical memory on the host machine. See [kernel.org documentation](https://www.kernel.org/doc/Documentation/vm/overcommit-accounting) for more information.

**Note:** If running in a distributed environment, make sure Redis server accepts connections by editing the `bind` line in /etc/redis/redis.conf or /etc/redis.conf.

#### Setting up virtual environment
You may need to upgrade pip:
```
pip install --upgrade pip
```

Install virtualenv:
```
pip install virtualenv
```

Create a virtualenv with python3:
```
virtualenv -p python3 rodssync
```

Activate virtual environment:
```
source rodssync/bin/activate
```

#### Install this package
```
pip install irods_capability_automated_ingest
```

Set up environment for Celery:
```
export CELERY_BROKER_URL=redis://<redis host>:<redis port>/<redis db> # e.g. redis://127.0.0.1:6379/0
export PYTHONPATH=`pwd`
```

Start celery worker(s):
```
celery -A irods_capability_automated_ingest.sync_task worker -l error -Q restart,path,file -c <num workers> 
```
**Note:** Make sure queue names match those of the ingest job (default queue names shown here).

#### Run tests
**Note:** The test suite requires Python version >=3.5.
**Note:** The tests should be run without running Celery workers.
```
python -m irods_capability_automated_ingest.test.test_irods_sync
```

#### Start sync job
```
python -m irods_capability_automated_ingest.irods_sync start <source dir> <destination collection>
```

#### List jobs
```
python -m irods_capability_automated_ingest.irods_sync list
```

#### Stop jobs
```
python -m irods_capability_automated_ingest.irods_sync stop <job name>
```

#### Watch jobs (same as using `--progress`)
```
python -m irods_capability_automated_ingest.irods_sync watch <job name>
```

### Intermediate: dockerize, manually config (needs to be updated for Celery)

`/tmp/event_handler.py`

```
from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):

    @staticmethod
    def target_path(session, meta, **options):
        return "/tmp/host" + path

```

`icommands.env`
```
IRODS_PORT=1247
IRODS_HOST=172.17.0.1
IRODS_USER_NAME=rods
IRODS_ZONE_NAME=tempZone
IRODS_PASSWORD=rods
```

```
docker run --rm --name some-redis -d redis:4.0.8
```

```
docker run --rm --link some-redis:redis irods_rq-scheduler:0.1.0 worker -u redis://redis:6379/0 restart path file
```

```
docker run --rm --link some-redis:redis -v /tmp/host/event_handler.py:/event_handler.py irods_capability_automated_ingest:0.1.0 start /data /tempZone/home/rods/data --redis_host=redis --event_handler=event_handler
```

```
docker run --rm --link some-redis:redis --env-file icommands.env -v /tmp/host/data:/data -v /tmp/host/event_handler.py:/event_handler.py irods_rq:0.1.0 worker -u redis://redis:6379/0 restart path file
```
### Advanced: kubernetes (needs to be updated for Celery)

This does not assume that your iRODS installation is in kubernetes.

#### `kubeadm`

setup `Glusterfs` and `Heketi`

create storage class

create a persistent volume claim `data`

#### install minikube and helm

set memory to at least 8g and cpu to at least 4

```
minikube start --memory 8192 --cpus 4
```

#### enable ingress on minikube

```
minikube addons enable ingress
```

#### mount host dirs

This is where you data and event handler. In this setup, we assume that your event handler is under `/tmp/host/event_handler` and you data is under `/tmp/host/data`. We will mount `/tmp/host/data` into `/host/data` in minikube which will mount `/host/data` into `/data` in containers,

`/tmp/host/data` -> minikube `/host/data` -> container `/data`.

and similarly,

`/tmp/host/event_handler` -> minikube `/host/event_handler` -> container `/event_handler`. Your setup may differ.

```
mkdir /tmp/host/event_handler
mkdir /tmp/host/data
```

`/tmp/host/event_handler/event_handler.py`
```python
from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):

    @staticmethod
    def target_path(session, meta, **options):
        return path

```

```
minikube mount /tmp/host:/host --gid 998 --uid 998 --9p-version=9p2000.L
```

#### enable incubator

```
helm repo add incubator https://kubernetes-charts-incubator.storage.googleapis.com/
```

#### build local docker images (optional)
If you want to use local docker images, you can build the docker images into minikube as follows:

`fish`
```
eval (minikube docker-env)
```

`bash`
```
eval $(minikube docker-env)
```

```
cd <repo>/docker/irods-postgresql
docker build . -t irods-provider-postgresql:4.2.2
```

```
cd <repo>/docker/irods-cockroachdb
docker build . -t irods-provider-cockroachdb:4.3.0
```

```
cd <repo>/docker
docker build . -t irods_capability_automated_ingest:0.1.0
```

```
cd <repo>/docker/rq
docker build . -t irods_rq:0.1.0
```

```
cd <repo>/docker/rq-scheduler
docker build . -t irods_rq-scheduler:0.1.0
```

#### install irods

##### `postgresql`
```
cd <repo>/kubernetes/irods-provider-postgresql
helm dependency update
```

```
cd <repo>/kubernetes
helm install ./irods-provider-postgresql --name irods
```

##### `cockroachdb`
```
cd <repo>/kubernetes/irods-provider-cockroachdb
helm dependency update
```

```
cd <repo>/kubernetes
helm install ./irods-provider-cockroachdb --name irods
```

when reinstalling, run

```
kubectl delete --all pv
kubectl delete --all pvc 
```

#### update irods configurations

Set configurations in `<repo>/kubernetes/chart/values.yaml` or `--set` command line argument.

#### install chart

```
cd <repo>/kubernetes/chart
```

We call our release `icai`.
```
cd <repo>/kubernetes
helm install ./chart --name icai
```

#### scale rq workers
```
kubectl scale deployment.apps/icai-rq-deployment --replicas=<n>
```

#### access by REST API (recommended)

##### submit job
`submit.yaml`
```yaml
root: /data
target: /tempZone/home/rods/data
interval: <interval>
append_json: <yaml>
timeout: <timeout>
all: <all>
event_handler: <event_handler>
event_handler_data: |
    from irods_capability_automated_ingest.core import Core
    from irods_capability_automated_ingest.utils import Operation

    class event_handler(Core):

        @staticmethod
        def target_path(session, meta, **options):
            return path

```

`fish`
```
curl -XPUT "http://"(minikube ip)"/job/<job name> -H "Content-Type: application/x-yaml" --data-binary=`submit.yaml`
```

`bash`
```
curl -XPUT "http://$(minikube ip)/job/<job name>" -H "Content-Type: application/x-yaml" --data-binary "@submit.yaml"
```

`fish`
```
curl -XPUT "http://"(minikube ip)"/job" -H "Content-Type: application/x-yaml" --data-binary "@submit.yaml"
```

`bash`
```
curl -XPUT "http://$(minikube ip)/job" -H "Content-Type: application/x-yaml" --data-binary "@submit.yaml"
```

##### list job
`fish`
```
curl -XGET "http://"(minikube ip)"/job"
```

`bash`
```
curl -XGET "http://$(minikube ip)/job"
```

##### delete job
`fish`
```
curl -XDELETE "http://"(minikube ip)"/job/<job name>"
```

`bash`
```
curl -XDELETE "http://$(minikube ip)/job/<job name>"
```

#### access by command line (not recommended)

##### submit job
```
kubectl run --rm -i icai --image=irods_capability_automated_ingest:0.1.0 --restart=Never -- start /data /tempZone/home/rods/data -i <interval> --event_handler=event_handler --job_name=<job name> --redis_host icai-redis-master
```

##### list job
```
kubectl run --rm -i icai --image=irods_capability_automated_ingest:0.1.0 --restart=Never -- list --redis_host icai-redis-master
```

##### delete job
```
kubectl run --rm -i icai --image=irods_capability_automated_ingest:0.1.0 --restart=Never -- stop <job name> --redis_host icai-redis-master
```

#### install logging tool

Install chart with set `log_level` to `INFO`.
```
helm del --purge icai
```

```
cd <repo>/kubernetes
helm install ./chart --set log_level=INFO --name icai
```

set parameters for elasticsearch

```
minikube ssh 'echo "sysctl -w vm.max_map_count=262144" | sudo tee -a /var/lib/boot2docker/bootlocal.sh'
minikube stop
minikube start
```

```
cd <repo>/kubernetes
helm install ./elk --name icai-elk
```


##### Grafana

look for service port
```
kubectl get svc icai-elk-grafana
```

forward port
```
kubectl port-forward svc/icai-elk-grafana 8000:80
```

If `--set grafana.adminPassword=""` system generates a random password, lookup admin password
```
kubectl get secret --namespace default icai-elk-grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo
```

open browser url `localhost:8000`

login with username `admin` and password `admin`
click on `icai dashboard`


##### Kibana

Uncomment kibana sections in the yaml files under the `<repo>/kubernetes/elk` directory

look for service port
```
kubectl get svc icai-elk-kibana
```

forward port
```
kubectl port-forward svc/icai-elk-kibana 8000:443
```

open browser url `localhost:8000`

