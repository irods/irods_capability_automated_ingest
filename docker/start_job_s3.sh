#! /bin/bash

usage() {
cat <<_EOF_
Usage: ./start_job.sh [OPTIONS]...

Example:

    ./start_job --src-dir <arg> --dest-coll <arg>

Starts an ingest job targeting the source directory and destination collection.

Available options:

    --env-file              Location of file containing iRODS environment variables. 
    --redis-container       The name of container running redis instance
    --redis-host            Hostname for redis instance
    --redis-port            Port for redis instance
    --redis-db              Database number to be used with redis instance
    --irods-host            Hostname for target iRODS server
    --irods-container       Name of container running iRODS server
    --s3-container          The name of container running S3 storage service
    --s3-endpoint-domain    S3 endpoint domain
    --s3-region_name        S3 region name
    --s3-proxy-url          URL to proxy for S3 access
    --src-dir               Full path to source S3 "folder"
    --dest-coll             Full path to desintation iRODS collection
    --ingest-options        Quoted string indicating options to pass to ingest application
    -h, --help              This message
_EOF_
    exit
}

#default values
env_file=icommands.env
irods_host=icat.example.org
redis_host=redis
redis_port=6379
redis_db=0
s3_keypair_file=s3_keypair
s3_endpoint_domain=s3.amazonaws.com
s3_region_name=us-east-1

while [ -n "$1" ]; do
    case "$1" in
        --env-file)              shift; env_file=${1};;
        --redis-container)       shift; redis_container=${1};;
        --redis-host)            shift; redis_host=${1};;
        --redis-port)            shift; redis_port=${1};;
        --redis-db)              shift; redis_db=${1};;
        --irods-host)            shift; irods_host=${1};;
        --irods-container)       shift; irods_container=${1};;
        --s3-container)          shift; s3_container=${1};;
        --s3-endpoint-domain)    shift; s3_endpoint_domain=${1};;
        --s3-region-name)        shift; s3_region_name=${1};;
        --s3-proxy-url)          shift; s3_proxy_url=${1};;
        --src-dir)               shift; src_dir=${1};;
        --dest-coll)             shift; dest_coll=${1};;
        --ingest-options)        shift; ingest_options=${1};;
        -h|--help)               usage;;
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

# Add S3 ingest options
ingest_options="${ingest_options} --s3_keypair /s3_keypair_file --s3_endpoint_domain ${s3_endpoint_domain} --s3_region_name ${s3_region_name}"

docker run \
    --rm \
    --env-file ${env_file} \
    -e "CELERY_BROKER_URL=${celery_broker_url}" \
    -v ${s3_keypair}:/s3_keypair_file:ro \
    ${container_links} \
    ingest \
    start \
    ${src_dir} \
    ${dest_coll} \
    ${ingest_options}
