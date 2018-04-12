FROM python:3.5

MAINTAINER Hao Xu version: 0.1.0

RUN pip install rq-scheduler==0.8.2

ENTRYPOINT ["rqscheduler"]
CMD ["-i", "1"]