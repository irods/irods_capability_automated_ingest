#! /bin/bash

usage() {
cat <<_EOF_
Usage: ./launch_worker.sh [OPTIONS]...

Example:

    ./launch_worker.sh --src-dir <arg> --concurrency 4

Stop a running ingest job.

Available options:

    --env-file              Location of file containing iRODS environment variables. 
    --redis-container       The name of container running redis instance
    --redis-host            Hostname for redis instance
    --redis-port            Port for redis instance
    --redis-db              Database number to be used with redis instance
    --irods-container       Name of container running iRODS server
    --irods-host            Hostname for target iRODS server
    --concurrency           Number of Celery workers to stand up
    -h, --help              This message
_EOF_
    exit
}

env_file=icommands.env
irods_host=icat.example.org
redis_host=redis
redis_port=6379
redis_db=0
concurrency=4

while [ -n "$1" ]; do
    case "$1" in
        --redis-container)  shift; redis_container=${1};;
        --redis-host)       shift; redis_host=${1};;
        --redis-port)       shift; redis_port=${1};;
        --redis-db)         shift; redis_db=${1};;
        --irods-host)       shift; irods_host=${1};;
        --irods-container)  shift; irods_container=${1};;
        --src-dir)          shift; src_dir=${1};;
        --concurrency)      shift; concurrency=${1};;
        -h|--help)          usage;;
    esac
    shift
done

celery_broker_url="redis://${redis_host}:${redis_port}/${redis_db}"

# Create a makeshift network between the containers, if present
if [[ ${irods_container} ]]; then
    container_links="--link ${irods_container}:${irods_host}"
fi

if [[ ${redis_container} ]]; then
    container_links="${container_links} --link ${redis_container}:${redis_host}"
fi

# Uses default values
docker run \
    --rm \
    --name workers \
    --env-file ${env_file} \
    -e "CELERY_BROKER_URL=${celery_broker_url}" \
    ${container_links} \
    -v ${src_dir}:/data:ro \
    ingest-worker \
    -c ${concurrency}
