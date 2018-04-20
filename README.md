# irods_rsync

## requirements ##

### setting up irods environment file ###

### redis ###
https://redis.io/topics/quickstart

start redis

```
redis-server
```

### virtualenv ###
```
pip3 install virtualenv
```

You may need to upgrade pip
```
pip3 install --upgrade pip
```

```
virtualenv rodssync
```

```
source rodssync/bin/activate
```

### clone repo ###

### rq ###
 * rq
 * rq-scheduler
 * python-redis-lock
```
pip install rq python-redis-lock rq-scheduler
```

make sure you are in the repo for the following commands
```
cd <repo dir>
```

start rqscheduler
```
rqscheduler -i 1
```

start rq worker(s)
```
rq worker restart path file
```

or
```
for i in {1..<n>}; do rq worker restart path file & done
```



### job monitoring ###
```
pip install rq-dashboard
```
```
rq-dashboard
```
or alternately, just use rq to monitor progress
```
rq info
```
### irods prc ###
```
pip install git+https://github.com/irods/python-irodsclient.git
```

tested with python 3.4+

## irods_sync ###

### run test ###


The tests should be run without running rq workers.

```
python -m irods_capability_automated_ingest.test.test_irods_sync
```

### start sync ###

If `-i` is not present, then only sync once

#### start
```
python -m irods_capability_automated_ingest.irods_sync start <local_dir> <collection> [-i <restart interval>] [ --event_handler <module name> ] [ --job_name <job name> ]
```

#### list restarting jobs
```
python -m irods_capability_automated_ingest.irods_sync list
```

#### stop
```
python -m irods_capability_automated_ingest.irods_sync stop <job name>
```

## available `--event_handler` methods

| method |  effect  | default |
| ----   |   ----- |  ----- |  
| pre_data_obj_create   |   user-defined python  |  none |
| post_data_obj_create   | user-defined python  |  none |
| pre_data_obj_modify     |   user-defined python   |  none |
| post_data_obj_modify     | user-defined python  |  none |
| pre_coll_create    |   user-defined python |  none |
| post_coll_create    |  user-defined python   |  none |
| as_user |   takes action as this iRODS user |  authenticated user |
| target_path | set mount path on the irods server which can be different from client mount path | client mount path |
| to_resource | defines  target resource request of operation |  as provided by client environment |
| operation | defines the mode of operation |  `Operation.REGISTER_SYNC` |

### operation mode ###

| operation |  new files  | updated files  |
| ----   |   ----- | ----- |
| `Operation.REGISTER_SYNC` (default)   |  registers in catalog | updates size in catalog |
| `Operation.REGISTER_AS_REPLICA_SYNC`  |   registers first or additional replica | updates size in catalog |
| `Operation.PUT`  |   copies file to target vault, and registers in catalog | no action |
| `Operation.PUT_SYNC`  |   copies file to target vault, and registers in catalog | copies entire file again, and updates catalog |
| `Operation.PUT_APPEND`  |   copies file to target vault, and registers in catalog | copies only appended part of file, and updates catalog |

`--event_handler` usage examples can be found [in the examples directory](irods_capability_automated_ingest/examples).


### Deployment

#### Basic: manual redis, rq-scheduler, pip
#### Intermediate: dockerize, manually config

`/tmp/mount.py`

```
from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):

    @staticmethod
    def target_path(session, target, path, **options):
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
docker run --rm --link some-redis:redis -v /tmp/host/mount.py:/mount.py -v /tmp/host/data:/data irods_rq-scheduler:0.1.0 worker -u redis://redis:6379/0 restart path file
```

```
docker run --rm --link some-redis:redis -v /tmp/host/mount.py:/mount.py irods_capability_automated_ingest:0.1.0 start /data /tempZone/home/rods/data --redis_host=redis --event_handler=mount
```

```
docker run --rm --link some-redis:redis --env-file icommands.env -v /tmp/host/data:/data -v /tmp/host/mount.py:/mount.py irods_rq:0.1.0 worker -u redis://redis:6379/0 restart path file
```
#### Advanced: kubernetes

##### install minikube and helm

##### mount host dirs

```
mkdir /tmp/host
mkdir /tmp/host/data
```

`/tmp/host/event_handler.py`
```
from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation

class event_handler(Core):

    @staticmethod
    def target_path(session, target, path, **options):
        return "/tmp/host" + path

```

```
minikube mount /tmp/host:/host
```

##### build local docker images (optional)

```
eval (minikube docker-env)
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

##### update irods password
Set the following to `irods` in `<repo>/kubernetes/chart/templates/irods-secret.yaml`
```
echo -n "rods" | base64
```

Set other configurations in `<repo>/kubernetes/chart/values.yaml`

##### install chart

```
cd <repo>/kubernetes/chart
helm dependency update
```

```
cd <repo>/kubernetes
helm install ./chart --set redis.usePassword=false --name icai
```

##### scale rq workers
```
kubectl scale deployment.apps/rq-deployment --replicas=<n>
```

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
