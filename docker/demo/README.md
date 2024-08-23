# Ingest Demo Compose Project

**DO NOT USE THIS IN PRODUCTION!!**

Use this Compose project to test out the ingest tool. There is a Docker volume shared between the iRODS service and the ingest workers that can be used for testing scans. There is another shared volume used to host the Minio storage.

It's easiest to try out scanning things from the `ingest-celery-workers` service instance.

## Build

```
docker compose build
```

The `ingest-celery-workers` service has a build argument that allows for controlling the version of the ingest package. Here's how to use it:

```
docker compose build --build-arg IRODS_AUTOMATED_INGEST_PIP_PACKAGE=git+https://github.com/irods/irods_capability_automated_ingest@main
```

This will clone the specified git repository and checkout the commit-ish specified. You could also specify a released version:

```
docker compose build --build-arg IRODS_AUTOMATED_INGEST_PIP_PACKAGE=irods-capability-automated-ingest==0.4.2
```

If no `--build-arg` is specified, the default build will install the latest released version of the package from PyPI. The following is equivalent to not specifying a `--build-arg` when building the project:
```
docker compose build --build-arg IRODS_AUTOMATED_INGEST_PIP_PACKAGE=irods-capability-automated-ingest
```

## Running the project

This demo simply starts the services and leaves them running with the expectation that commands will be issued to them either through `docker exec` or via client requests to the various endpoints.

It is a simple project, so starting and stopping it are straightforward.

To bring the project up:

```
docker compose up
```

To bring the project down:

```
docker compose down
```

The other `docker compose` commands (`start`, `stop`, `restart`, etc.) should work as expected, as well.

If you wish to adjust the Celery concurrency, modify the Compose YAML file to adjust the `command` run by the `ingest-celery-workers` service:
```yaml
command: ["-c", "2"] # Adjust the "2" value to whatever concurrency you want
```
The `command` can only be adjusted before the container is created, so if you wish to adjust the concurrency after the project is already up, you will need to recreate the `ingest-celery-workers` service instance containers.

## Scanning an S3 bucket

Change the port exposed by the `minio` service, if needed, so that the MinIO Console can be accessed. The MinIO server is being run with access key `irods` and secret key `irodsadmin`. The place from which the job is launched should have a keypair file with these credentials:
```
irods
irodsadmin
```

To perform a basic scan of an S3 bucket called, for example, `ingest-test-bucket`, run something like the following:

```
python3 -m irods_capability_automated_ingest.irods_sync start \
    /ingest-test-bucket \
    /tempZone/home/rods/ingest-test-bucket \
    --s3_keypair /path/to/s3keypair.txt \
    --s3_endpoint_domain minio:19000 \
    --s3_insecure_connection \
    --synchronous \
    --progress
```

It's easiest to try out scanning things from the `ingest-celery-workers` service instance.
