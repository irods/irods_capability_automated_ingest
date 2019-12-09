#! /bin/bash

usage() {
cat <<_EOF_
Usage: ./run_tests.sh [OPTIONS]...

Runs the test suite.

Available options:

    --env-file              Location of file containing iRODS environment variables. 
    --redis-container       The name of container running redis instance
    --redis-host            Hostname for redis instance
    --redis-port            Port for redis instance
    --redis-db              Database number to be used with redis instance
    --irods-host            Hostname for target iRODS server
    --irods-container       Name of container running iRODS server
    --specific-test         Dot-separated specific test to run
    --host-mount-dir        Full path to directory on host for placing files
    -h, --help              This message
_EOF_
    exit
}

env_file=icommands.env
irods_host=icat.example.org
redis_host=redis
redis_port=6379
redis_db=0
host_mount_dir=/mnt/ingest_test_mount

while [ -n "$1" ]; do
    case "$1" in
        --env-file)              shift; env_file=${1};;
        --redis-container)       shift; redis_container=${1};;
        --redis-host)            shift; redis_host=${1};;
        --redis-port)            shift; redis_port=${1};;
        --redis-db)              shift; redis_db=${1};;
        --irods-host)            shift; irods_host=${1};;
        --irods-container)       shift; irods_container=${1};;
        --specific-test)         shift; specific_test=${1};;
        --host-mount-dir)        shift; host_mount_dir=${1};;
        -h|--help)               usage;;
    esac
    shift
done

#celery_broker_url="redis://${redis_host}:${redis_port}/${redis_db}"

# Create a makeshift network between the containers, if present

# Running iRODS 4.2.6
# docker run --name irods-box -d -v /mnt/ingest_test_mount:/tmp/testdir:ro --hostname icat.example.org irods_4.2.6
if [[ ${irods_container} ]]; then
    container_links="--link ${irods_container}:${irods_host}"
fi

# Running Redis
# docker run --rm --name some-redis -d redis:4.0.8
if [[ ${redis_container} ]]; then
    container_links="${container_links} --link ${redis_container}:${redis_host}"
fi

if [[ ${specific_test} ]]; then
    test_case="-e TEST_CASE=irods_capability_automated_ingest.test.test_irods_sync.${specific_test}"
fi

docker run \
    --rm \
    --env-file ${env_file} \
    ${test_case} \
    ${container_links} \
    -v ${host_mount_dir}:/tmp/testdir \
    ingest-test
echo $?
