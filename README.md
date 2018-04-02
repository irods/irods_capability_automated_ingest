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
pip install rq python-redis-lock
```

As of this writing `rq-scheduler` doesn't work for this config because of argparse conflict. Use the following pull request instead.
```
pip install git+git://github.com/sourcepirate/rq-scheduler@9166f30d11849ebe60aacc94c6d072184de55b1d
```

make sure you are in the repo in the following commands
```
cd <repo dir>
```

start rqscheduler
```
rqscheduler -i 1
```

start rq workers
```
rq worker restart path file
```

or
```
for i in {1..<n>}; do rq worker restart path file & done
```



### (optional) rq-dashboard ###
```
pip install rq-dashboard
```
```
rq-dashboard
```
### irods prc ###
```
pip install git+https://github.com/irods/python-irodsclient.git
```

tested under python 3.5

## irods rsync ###

### run test ###

```
python -m test.test_irods_sync
```

### start sync ###

If `-i` is not present, then only sync once

#### start
```
python irods_sync.py start <local_dir> <collection> [-i <restart interval>] [ --event_handler <module name> ] [ --job_name <job name> ]
```

#### list restarting jobs
```
python irods_sync.py list
```

#### stop
```
python irods_sync.py stop <job name>
```

### event handler ###

```
{pre,post}_data_obj_{create,modify}(hdlr_mod, session, target, source, **options):
```
  
```
{pre,post}_coll_create(hdlr_mod, session, target, source, **options):
```

```
as_user(target, source, **options):
```

```
to_resource(session, target, source, **options):
```
used by
 * register
 * update
 * update to determine register resource
 * when `as_replica` returns `True` determine whether it is a register or a update
 
```
to_root_resource(session, target, source, **options):
```
used by
 * upload
 * sync

```
to_resource_hier(session, target, source, **options):
```
(optional) used by
 * register
 * update

```
as_replica(session, target, source, **options):
```
default: `False`

```
put(session, target, source, **options):
```
default: `False`

```
sync(session, target, source, **options):
```
used by
  * sync
default: `True`

```
append(session, target, source, **options):
```
used by
  * sync
default: `False`

example: `evhdlr.py`

### features
