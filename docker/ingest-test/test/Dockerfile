FROM python:3.11

RUN apt update && apt install -y netcat-traditional

COPY irods_environment.json /

ENV TEST_CASE=${TEST_CASE}

COPY run_tests.sh /
RUN chmod u+x /run_tests.sh
ENTRYPOINT ["./run_tests.sh"]
