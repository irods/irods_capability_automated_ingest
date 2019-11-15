#! /bin/bash

usage() {
cat <<_EOF_
Usage: ./list_jobs.sh [OPTIONS]...

Lists running ingest jobs.

Available options:

    --redis-container       The name of container running redis instance
    --redis-host            Hostname for redis instance
    --redis-port            Port for redis instance
    --redis-db              Database number to be used with redis instance
    -h, --help              This message
_EOF_
    exit
}

redis_host=redis
redis_port=6379
redis_db=0

while [ -n "$1" ]; do
    case "$1" in
        --redis-container)  shift; redis_container=${1};;
        --redis-host)       shift; redis_host=${1};;
        --redis-port)       shift; redis_port=${1};;
        --redis-db)         shift; redis_db=${1};;
        --ingest-options)   shift; ingest_options=${1};;
        -h|--help)          usage;;
    esac
    shift
done

celery_broker_url="redis://${redis_host}:${redis_port}/${redis_db}"

if [[ ${redis_container} ]]; then
    container_links="--link ${redis_container}:${redis_host}"
fi

docker run \
    --rm \
    -e "CELERY_BROKER_URL=${celery_broker_url}" \
    ${container_links} \
    ingest \
    list \
    ${ingest_options}
