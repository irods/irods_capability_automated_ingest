FROM python:3.10

RUN apt update && apt install -y netcat

COPY irods_environment.json /

ENV TEST_CASE=${TEST_CASE}

COPY run_tests.sh /
RUN chmod u+x /run_tests.sh
ENTRYPOINT ["./run_tests.sh"]
