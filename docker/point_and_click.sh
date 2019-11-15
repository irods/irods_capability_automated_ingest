usage() {
cat <<_EOF_
Usage: ./point_and_click.sh [OPTIONS]...

Stand up Celery workers, run an ingest job to completion, and tear down Celery workers.

Available options:

    --env-file              Location of file containing iRODS environment variables. 
    --redis-container       The name of container running redis instance
    --irods-container       Name of container running iRODS server
    --src-dir               Full path to source directory on host machine to be scanned
    --dest-coll             Full path to desintation iRODS collection
    -h, --help              This message
_EOF_
    exit
}

while [ -n "$1" ]; do
    case "$1" in
        --env-file)              shift; env_file=${1};;
        --redis-container)       shift; redis_container=${1};;
        --irods-container)       shift; irods_container=${1};;
        --src-dir)               shift; src_dir=${1};;
        --dest-coll)             shift; dest_coll=${1};;
        -h|--help)               usage;;
    esac
    shift
done

# Create a makeshift network between the containers, if present
if [[ ${irods_container} ]]; then
    worker_options="--irods-container ${irods_container}"
fi

if [[ ${redis_container} ]]; then
    worker_options="${worker_options} --redis-container ${redis_container}"
fi

worker_options="${worker_options} --src-dir ${src_dir}"

ingest_options="--synchronous --progress --ignore_cache"
start_job_options="${worker_options} --dest-coll ${dest_coll} --ingest-options ${ingest_options}"

./launch_worker.sh ${worker_options} &
./start_job.sh ${start_job_options}
docker stop workers
echo $?
