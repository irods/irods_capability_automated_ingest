# irods_rsync

requirements:

 * setting up irods environment file
 * redis
 * rq
 * rq-scheduler
 * redis_lock
 * irods prc

tested under python 3.5

start redis

```
./redis-server
```

start scheduler

```
rqscheduler
```

start worker

```
rq worker restart path file 
```

start sync

```
python start_sync.py <local_dir> <collection> -i <restart interval>
```
