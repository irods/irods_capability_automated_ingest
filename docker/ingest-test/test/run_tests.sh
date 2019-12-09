#! /bin/bash

pip install --upgrade ${PIP_PACKAGE}

# Wait until the provider is up and accepting connections.
until nc -z icat.example.org 1247; do
    sleep 1
done

sleep 10

python -m unittest ${TEST_CASE}
