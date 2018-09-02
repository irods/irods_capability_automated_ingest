# iRODS Automated Ingest Framework

The automated ingest framework gives iRODS an enterprise solution that solves two major use cases: getting existing data under management and ingesting incoming data hitting a landing zone.

Based on the Python iRODS Client and Celery, this framework can scale up to match the demands of data coming off instruments, satellites, or parallel filesystems.

The example diagrams below show a filesystem scanner and a landing zone.

![Automated Ingest: Filesystem Scanner Diagram](capability_automated_ingest_filesystem_scanner.jpg)

![Automated Ingest: Landing Zone Diagram](capability_automated_ingest_landing_zone.jpg)

## Usage options

### available `--event_handler` methods

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
| max_retries | defines max retries | 0
| timeout | defines timeout | 3600
| delay | | 

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

**Note:** If running on different computers, make sure Redis server accepts connections by editing the `bind` line in /etc/redis/redis.conf or /etc/redis.conf.

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
**Note:** You will need python version >= 3.5 to run the test suite

Activate virtual environment:
```
source rodssync/bin/activate
```

#### Clone this repo
```
git clone https://github.com/irods/irods_capability_automated_ingest --branch icai-celery
```

#### Installing and configuring Celery
 * celery[redis]
 * progressbar2
 * python-redis-lock
 * python-irodsclient
 * scandir
 * structlog
```
pip install celery[redis] progressbar2 python-redis-lock scandir python-irodsclient structlog
```

Make sure you are in the repo and on the icai-celery branchfor the following commands:
```
cd <repo dir>
```

Set up environment for Celery:
`fish`
```
set -xl CELERY_BROKER_URL redis://<redis host>:<redis port>/<redis db> # e.g. redis://127.0.0.1:6379/0
set -xl PYTHONPATH (pwd)
```
`bash`
```
export CELERY_BROKER_URL=redis://<redis host>:<redis port>/<redis db> # e.g. redis://127.0.0.1:6379/0
export PYTHONPATH=`pwd`
```

Start celery worker(s):
```
celery -A irods_capability_automated_ingest.sync_task worker -l error -Q restart,path,file -c <num workers> 
```

#### Run tests
**Note:** The tests should be run without running Celery workers.
```
python -m irods_capability_automated_ingest.test.test_irods_sync
```

#### Start sync job
```
python -m irods_capability_automated_ingest.irods_sync start <source dir> <destination collection>
```

Usage:
```
irods_sync.py start [-h] [-i INTERVAL] [--file_queue FILE QUEUE]
                           [--path_queue PATH QUEUE]
                           [--restart_queue RESTART QUEUE]
                           [--event_handler EVENT HANDLER]
                           [--job_name JOB NAME] [--append_json APPEND JSON]
                           [--ignore_cache] [--initial_ingest] [--synchronous]
                           [--progress] [--profile] [--list_dir]
                           [--scan_dir_list]
                           [--exclude_file_type EXCLUDE_FILE_TYPE]
                           [--exclude_file_name EXCLUDE_FILE_NAME [EXCLUDE_FILE_NAME ...]]
                           [--exclude_directory_name EXCLUDE_DIRECTORY_NAME [EXCLUDE_DIRECTORY_NAME ...]]
                           [--irods_idle_disconnect_seconds DISCONNECT IN SECONDS]
                           [--redis_host REDIS HOST]
                           [--redis_port REDIS PORT]
                           [--redis_db REDIS DB]
                           [--log_filename LOG FILE]
                           [--log_when LOG WHEN]
                           [--log_interval LOG INTERVAL]
                           [--log_level LOG LEVEL]
                           [--profile_filename PROFILE FILE]
                           [--profile_when PROFILE WHEN]
                           [--profile_interval PROFILE INTERVAL]
                           [--profile_level PROFILE LEVEL]
                           ROOT TARGET

positional arguments:
  ROOT                  root directory
  TARGET                target collection

optional arguments:
  -h, --help            show this help message and exit
  -i INTERVAL, --interval INTERVAL
                        restart interval (in seconds)
  --file_queue FILE QUEUE
                        file queue
  --path_queue PATH QUEUE
                        path queue
  --restart_queue RESTART QUEUE
                        restart queue
  --event_handler EVENT HANDLER
                        event handler
  --job_name JOB NAME   job name
  --append_json APPEND JSON
                        append json
  --ignore_cache        ignore cache
  --initial_ingest      initial ingest
  --synchronous         synchronous
  --progress            progress
  --profile             profile
  --list_dir            list dir
  --scan_dir_list       scan dir list
  --exclude_file_type EXCLUDE_FILE_TYPE
                        types of files to exclude: regular, directory,
                        character, block, socket, pipe, link
  --exclude_file_name EXCLUDE_FILE_NAME [EXCLUDE_FILE_NAME ...]
                        a list of space-separated python regular expressions
                        defining the file names to exclude such as
                        "(\S+)exclude" "(\S+)\.hidden"
  --exclude_directory_name EXCLUDE_DIRECTORY_NAME [EXCLUDE_DIRECTORY_NAME ...]
                        a list of space-separated python regular expressions
                        defining the directory names to exclude such as
                        "(\S+)exclude" "(\S+)\.hidden"
  --irods_idle_disconnect_seconds DISCONNECT IN SECONDS
                        irods disconnect time in seconds
  --redis_host REDIS HOST
                        redis host
  --redis_port REDIS PORT
                        redis port
  --redis_db REDIS DB   redis db
  --log_filename LOG FILE
                        log filename
  --log_when LOG WHEN   log when
  --log_interval LOG INTERVAL
                        log interval
  --log_level LOG LEVEL
                        log level
  --profile_filename PROFILE FILE
                        profile filename
  --profile_when PROFILE WHEN
                        profile when
  --profile_interval PROFILE INTERVAL
                        profile interval
  --profile_level PROFILE LEVEL
                        profile level
```

If `-i` is not present, then only sync once.

The `--append_json` is stored in `job.meta["append_json"]`.

The `--ignore_cache` ignores cached last sync time.

The `--synchronous` will block until job is done.

The `--progress` will show progress bar when `--synchronous` is enabled.

The `--list_dir` will use cause the tasks to `listdir` instead of `scandir`.

The `--log_filename` specify profile file name.

The `--log_level` specify the profile level, currently should specify `INFO`

The `--profile` will use enable profiling.

The `--profile_filename` specify profile file name.

The `--profile_level` specify the profile level, currently should specify `INFO`

The `--redis_host`, `--redis_port`, and `--redis_db` specify information about your Redis server (default values used if unspecified).


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

