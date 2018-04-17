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

 * Basic: manual redis, rq-scheduler, pip
 * Intermediate: dockerize, manually config
 * Advanced: kubernetes

