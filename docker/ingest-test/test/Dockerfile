FROM python:3.5

RUN apt update && apt install -y netcat

ARG PIP_PACKAGE="irods-capability-automated-ingest"

RUN pip install ${PIP_PACKAGE}

COPY irods_environment.json /

ENV TEST_CASE=${TEST_CASE}

COPY run_tests.sh /
RUN chmod u+x /run_tests.sh
ENTRYPOINT ["./run_tests.sh"]
