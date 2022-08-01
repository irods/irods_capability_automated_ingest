# How to run the test suite using docker-compose

## Step 1: Build the images

Run the following to build the required images:
```
docker-compose build
```
When testing against an alternative version of iRODS (default is currently 4.3.0), there are three variables in the
`docker-compose.yml` file which must be changed prior to the build step. For example, if testing against iRODS 4.2.11:
```
  irods-catalog-provider:
    build:
      args:
          irods_version: 4.2.11-1~bionic
          irods_version_major_minor: 4.2
          py_version: ""
```
Note that, depending on whether the iRODS major/minor version is 4.2 or 4.3, the `py_version` takes on the possible values `""` or `"3"`, respectively.

## Step 2: Run the project

Bring up the docker-compose project and the test suite will run on its own:
```
docker-compose --env-file icommands.env up
```
The test suite is one of the services of the docker-compose project, so it will run on its own. The container is tied to the tests running, so it will exit once completed.
The `--env-file` option is required in order to correctly configure the environment for the tests.

## Step 3: Bring down the project

The project is not made to come down by itself (yet), so it has to be brought down after each run:
```
docker-compose down
```

A script is forthcoming which should make this a little easier to run in the future.

# How to run the test suite using docker

This process is very similar to that of running an ingest job in docker. It is recommended to see [docker/README.md](docker/README.md).

The ingest-test image is based on the ingest docker image found in the docker directory. As such, many of the same requirements hold when running the tests as when a normal ingest job takes place.

## Step 0: Preparing the ground
An iRODS server is assumed to be running on an accessible network with hostname `icat.example.org` listening on port 1247 (default values).
The iRODS server is ssumed to have a rodsadmin user named `rods` with password `rods` on zone `tempZone` (default values).

A Redis server is assumed to be running on an accessible network with hostname `redis` listening on port 6379 with an unused database 0 (default values).
The test suite starts and stops its own workers, so it is advisable to stop any workers pointed at the Redis server you plan to use, or use a different database.

If you don't want to run the tests in a distributed fashion, give `localhost` the needed aliases to refer to the Redis and iRODS servers.

## Step 1: Build Docker images
Build the ingest image:
```
$ docker build -t ingest ..
```

If you would like to install with a git repo, you can provide a pip-style Git URL to the `PIP_PACKAGE` build argument (e.g.):
```
docker build -t ingest --build-arg "PIP_PACKAGE=git+https://github.com/irods/irods_capability_automated_ingest@master"
```

Build the ingest-test image (uses ingest image as a base so as to be identical):
```
$ docker build -t ingest-test -f Dockerfile.test .
```

## Step 2: Run the tests:
Need a file like this from wherever the tests are launched:
```
$ cat icommands.env
IRODS_PORT=1247
IRODS_HOST=icat.example.org
IRODS_USER_NAME=rods
IRODS_ZONE_NAME=tempZone
IRODS_PASSWORD=rods
```
This will serve as the iRODS client authentication (rather than running `iinit` somewhere).
Also need a valid irods_environment.json file.

The default values used by the tests for iRODS interactions are as shown above and should not be changed.
This is not built into the image because flexibility for server and user information will be provided in the future.

To run the full test suite, run this:
```
docker run --rm --env-file icommands.env -v /path/to/mountdir:/tmp/testdir ingest-test
```
`/path/to/mountdir` must be accessible to both the ingest-test container and `icat.example.org`. `/tmp/testdir` is hard-coded in the tests - do not change this.

To run a specfic test, set the `TEST_CASE` environment variable to the desired test (dot-notated) when running the container (e.g.):
```
docker run --rm --env-file icommands.env -v /path/to/mountdir:/tmp/testdir -e TEST_CASE=test_irods_sync.Test_register.test_register ingest-test
```

The provided `run_tests.sh` script is a wrapper around the `docker run` commands to help with running the tests.
This example usage shows running a specific test with local iRODS and Redis containers which will be linked to the container running the tests:
```
run_tests.sh --redis-container some-redis --irods-container irods-box --host-mount-dir /path/to/mountdir --specific-test Test_irods_sync_UnicodeEncodeError.test_register
```
