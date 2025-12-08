# How to run the test suite using docker-compose

## Step 1: Build the images

Run the following to build the required images:
```
docker compose build
```
When testing against an alternative version of iRODS, there are three variables in the `docker-compose.yml` file which must be changed prior to the build step. For example, if testing against iRODS 4.3.3:
```
  irods-catalog-provider:
    build:
      args:
          irods_version: 4.3.3-0~jammy
          irods_version_major_minor: 4.3
          py_version: 3
```
Note that, depending on whether the iRODS major/minor version is 4.2 or 4.3, the `py_version` takes on the possible values `""` or `"3"`, respectively.

## Step 2: Run the project

Bring up the docker-compose project and the test suite will run on its own:
```
docker compose --env-file icommands.env up --abort-on-container-exit
```
The test suite is one of the services of the docker-compose project, so it will run on its own. The container is tied to the tests running, so it will exit once completed. The `--env-file` option is required in order to correctly configure the environment for the tests.

If you wish to leave the other services running after the test run, omit the `--abort-on-container-exit` option in the command above.

## Step 3: Cleaning up after tests finish

The tests expect to have a clean environment each time they are run. As such, we need to make sure to clean up after each test run. This can be done like this:
```
docker compose down
```
