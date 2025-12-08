#! /bin/bash -ex

pip install ${PIP_PACKAGE}

# Wait until the provider is up and accepting connections.
until nc -z irods-catalog-provider 1247; do
    sleep 1
done

sleep 10

python -m unittest -v ${TEST_CASE}
