FROM python:3.5

MAINTAINER Hao Xu version: 0.1.0

RUN pip install redis==2.10.6 rq==0.10.0 rq-scheduler==0.8.2 python-redis-lock==3.2.0
RUN pip install git+https://github.com/irods/python-irodsclient
RUN pip install git+https://github.com/irods/irods_capability_automated_ingest

ENTRYPOINT ["irods_capability_automated_ingest"]
CMD ["-h"]