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
pip install https://github.com/sourcepirate/rq-scheduler@9166f30d11849ebe60aacc94c6d072184de55b1d
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
rq worker -v restart path file
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

### start sync ###

```
python start_sync.py <local_dir> <collection> -i <restart interval>
```
