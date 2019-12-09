#! /bin/bash

set -e

docker-compose --env-file icommands.env up &

until [ $(docker container inspect -f '{{.State.Status}}' ingest-test_ingest-test_1) ]; do
    #echo "waiting for container to exist"
    sleep 1
done

while [ ! $(docker container inspect -f '{{.State.Status}}' ingest-test_ingest-test_1) == "running" ]; do
    #echo "waiting for container to run"
    sleep 1
done

#echo "test container is up"

while [ $(docker container inspect -f '{{.State.Status}}' ingest-test_ingest-test_1) == "running" ]; do
    #echo "waiting for tests to finish"
    sleep 1
done

docker-compose down

