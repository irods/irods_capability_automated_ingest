FROM python:3.11

ARG IRODS_AUTOMATED_INGEST_PIP_PACKAGE="irods-capability-automated-ingest"

RUN pip install ${IRODS_AUTOMATED_INGEST_PIP_PACKAGE}

ENTRYPOINT ["celery", "-A", "irods_capability_automated_ingest", "worker", "-Q", "restart,path,file"]

# Override the command at runtime to adjust Celery concurrency and other options.
CMD ["-c", "2"]
