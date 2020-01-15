#! /bin/bash

usage() {
cat <<_EOF_
Usage: ./launch_worker.sh [OPTIONS]...

Example:

    ./launch_worker.sh --src-dir <arg> --concurrency 4

Stop a running ingest job.

Available options:

    --env-file              Location of file containing iRODS environment variables. 
    --s3-keypair-file       Location of file containing an S3 keypair
    --redis-container       The name of container running redis instance
    --redis-host            Hostname for redis instance
    --redis-port            Port for redis instance
    --redis-db              Database number to be used with redis instance
    --irods-host            Hostname for target iRODS server
    --irods-container       Name of container running iRODS server
    --s3-container          The name of container running S3 storage service
    --s3-endpoint-domain    S3 endpoint domain
    --src-dir               Full path to source S3 "folder"
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
s3_keypair_file=s3_keypair
s3_endpoint_domain=minio
concurrency=4

while [ -n "$1" ]; do
    case "$1" in
        --env-file)             shift; env_file=${1};;
        --redis-container)      shift; redis_container=${1};;
        --redis-host)           shift; redis_host=${1};;
        --redis-port)           shift; redis_port=${1};;
        --redis-db)             shift; redis_db=${1};;
        --irods-host)           shift; irods_host=${1};;
        --irods-container)      shift; irods_container=${1};;
        --s3-keypair-file)      shift; s3_keypair_file=${1};;
        --s3-container)         shift; s3_container=${1};;
        --s3-endpoint-domain)   shift; s3_endpoint_domain=${1};;
        --src-dir)              shift; src_dir=${1};;
        --concurrency)          shift; concurrency=${1};;
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

if [[ ${s3_container} ]]; then
    container_links="${container_links} --link ${s3_container}:${s3_endpoint_domain}"
fi

# Uses default values
docker run \
    --rm \
    --name workers \
    --env-file ${env_file} \
    -e "CELERY_BROKER_URL=${celery_broker_url}" \
    ${container_links} \
    -v ${src_dir}:/data:ro \
    -v ${s3_keypair}:/s3_keypair_file:ro \
    ingest-worker \
    -c ${concurrency}
