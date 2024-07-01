FROM python:3.11

ARG PIP_PACKAGE="irods-capability-automated-ingest"

RUN pip install ${PIP_PACKAGE}

ENTRYPOINT ["celery", "-A", "irods_capability_automated_ingest.sync_task", "worker", "-Q", "restart,path,file"]
#CMD ["-h"]
