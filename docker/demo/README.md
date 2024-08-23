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

## Performance testing

While using Docker is not going to get you the best possible performance numbers, it can be useful for benchmarking certain tasks in a reproducible environment.

This section will describe some interesting things you can do to test out various configurations for performance.

### Celery configuration

As mentioned in other sections, the `concurrency` configuration can be changed before container creation for the `ingest-celery-workers` service by overriding the `command` in the Docker Compose YAML file. This affects the number of Celery workers in a given service instance.

Celery has a number of other configurations for the workers which can help with performance: [https://docs.celeryq.dev/en/stable/userguide/configuration.html#worker](https://docs.celeryq.dev/en/stable/userguide/configuration.html#worker)

### Docker Compose service scaling

The `ingest-celery-workers` service can be "scaled up" using the `--scale` option of `docker compose up`. The default scale is 1 service instance, but the scale can be adjusted like this:
```bash
docker compose up --scale ingest-celery-workers=4 # replace 4 with desired number of instances
```
The above line will spawn 4 instances (containers) of the `ingest-celery-workers` service with each instance having a `concurrency` of whatever has been configured. With the default configuration, this would be 2, for a total of 8 workers across the 4 containers. This can even be done when the project is already up to scale the number of instances up without affecting the existing containers. This can of course be used to scale *down* the number of instances as well.

### Network manipulation with Traffic Control (`tc`)

`tc` can be used to simulate network delays and other networking conditions that may not ordinarily be present. See the `tc` documentation for more information: [https://linux.die.net/man/8/tc](https://linux.die.net/man/8/tc)

Network traffic manipulation requires enabling the additional capability `NET_ADMIN` in the target containers. Remember that "additional capabilities" can only be added at container creation. This can be done a number of different ways, but the simplest way for this project is to add the following `cap_add` stanza to the `ingest-celery-workers` service in the Docker Compose YAML file:
```yaml
cap_add:
    - NET_ADMIN
```

Here are some useful commands to try executing inside the `ingest-celery-workers` service instance containers for manipulating network traffic:
```bash
tc qdisc add dev eth0 root netem delay 100ms # to add rule
tc qdisc show dev eth0 # to show rules
tc qdisc del dev eth0 root netem # to delete rule
```
Note: In order to run `tc`, the proper package must be installed in the container(s) in which the command will be running. For most Linux distributions, this is `iproute2`.
