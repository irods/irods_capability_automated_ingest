FROM python:3.5

ARG PIP_PACKAGE="irods-capability-automated-ingest"

RUN pip install ${PIP_PACKAGE}

COPY irods_environment.json /

ENTRYPOINT ["irods_capability_automated_ingest"]
CMD ["-h"]
