FROM python:3.5

MAINTAINER Hao Xu version: 0.1.0

RUN pip install redis rq rq_scheduler python-redis-lock
RUN pip install git+https://github.com/irods/python-irodsclient
RUN pip install git+https://github.com/irods/irods_capability_automated_ingest

ENTRYPOINT ["irods_capability_automated_ingest"]
CMD ["-h"]