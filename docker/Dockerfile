FROM python:3.5

RUN pip install irods-capability-automated-ingest

COPY irods_environment.json /

ENTRYPOINT ["irods_capability_automated_ingest"]
CMD ["-h"]
