#! /bin/bash

PIP_INSTALL="irods-capability-automated-ingest"
if [[ ${1} ]]; then
    PIP_INSTALL=${1}
fi

BUILD_ARG="PIP_PACKAGE=${PIP_INSTALL}"

docker build --build-arg ${BUILD_ARG} -t ingest .
docker build -t ingest-worker -f Dockerfile.ingest_worker .
