# How to dockerize with manual config

An iRODS server is assumed to be running on an accessible network.

## Step 1: Build
Build the ingest image:
```
$ docker build -t ingest .
```

Build the Celery worker image (uses ingest image as a base so as to be identical):
```
$ docker build -t ingest-workers -f Dockerfile.ingest_worker .
```

## Step 2: Start redis server

Start redis server in one container:
```
$ docker run --rm --name some-redis -d redis:4.0.8
```

## Step 3: Start Celery workers
Need a file like this from wherever the containers are launched:
```
$ cat icommands.env
IRODS_PORT=1247
IRODS_HOST=irods-provider
IRODS_USER_NAME=rods
IRODS_ZONE_NAME=tempZone
IRODS_PASSWORD=rods
```
This will serve as the iRODS client authentication (rather than running `iinit` somewhere).
Also need a valid irods_environment.json file.

This line will start 4 Celery workers with read-only access to a directory on the host machine:
```
docker run --rm --name workers -e "CELERY_BROKER_URL=redis://redis:6379/0" --env-file icommands.env --link some-redis:redis --link irods-provider:icat.example.org -v /path/to/ingest:/mount/path:ro ingest-worker -c 4
```
`CELERY_BROKER_URL` is required here in order for the sync tasks to run properly. Replace the URL with the hostname where your redis server is running. `icommands.env` needs to be present somewhere on the machine launching the container in order to authenticate with iRODS server.

The mounted directory path may not need to be read-only depending on your use-case (i.e. landing zone); but, each set of Celery workers must be mounted on the path to scan.

## Step 4: Run an ingest job

Starts a synchronous ingest job with a progress bar:
```
docker run --rm -e "CELERY_BROKER_URL=redis://redis:6379/0" --env-file icommands.env --link some-redis:redis --link irods-provider:icat.example.org -v /path/to/ingest:/mount/path:ro ingest start /mount/path /tempZone/home/rods/destination_collection --synchronous --progress
```

You can also list, watch, and stop running jobs as per usual by replacing start with whatever you want to run:
```
docker run --rm -e "CELERY_BROKER_URL=redis://redis:6379/0" --link some-redis:redis ingest list
```
