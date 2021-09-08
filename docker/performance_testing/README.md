variables for performance tests:
1. code that is running
2. number of files ingested
3. traffic control delay
4. number of workers (nodes x --concurrency?)

- Prerequisites
Running this project has a dependency on the [iRODS Testing Environment](https://github.com/irods/irods_testing_environment).

```
# set up virtualenv
virtualenv -p python3 ~/ingest_performance_testing_env
# install docker-compose so we can use pieces of the testing environment
pip install docker-compose ; # recommended: virtualenv!
# build the project (change SHA/repo to match what you are testing)
docker-compose --build-arg PIP_PACKAGE=git+https://github.com/irods/irods_capability_automated_ingest@master build ;
# stand up the project
docker-compose up -d ;
# install iRODS packages
docker exec -e DEBIAN_FRONTEND=noninteractive performance_testing_irods-catalog-provider_1 apt install -y irods-database-plugin-postgres ;
# this is not strictly speaking necessary, but you do need this repo somewhere on the host machine
git clone https://github.com/irods/irods_testing_environment ;
# setup iRODS server using default settings
python ./irods_testing_environment/setup.py --project-directory ./irods_capability_automated_ingest/docker/performance_testing -d postgres:10.12 -p ubuntu:18.04 --exclude-irods-catalog-consumers-setup -v ;
```

- Turn some knobs
```
# install some networking tools (ingest-worker service runs with NETWORK_ADMIN enabled)
docker exec performance_testing_ingest-worker_1 apt update && apt install -y iputils-ping iproute2 ;
# some examples of what you can do with tc (traffic controller)
docker exec performance_testing_ingest-worker_1 tc qdisc add dev eth0 root netem delay 100ms # to add rule
docker exec performance_testing_ingest-worker_1 tc qdisc show dev eth0 # to show rules
docker exec performance_testing_ingest-worker_1 tc qdisc del dev eth0 root netem # to delete rule
# scale workers (up or down)
docker-compose up --scale ingest-worker=N
```

- Run an ingest job
```
docker exec -e CELERY_BROKER_URL=redis://some-redis:6379/0 performance_testing_ingest-worker_1 python -m irods_capability_automated_ingest.irods_sync start --synchronous --progress /wormhole /tempZone/home/rods/ingested # plus whatever else
```

In this example, we are running the ingest job inside an ingest-worker service instance container, but you can also run this on the host machine if you so desire (need to configure networking with Redis and map the volume mounts appropriately).
