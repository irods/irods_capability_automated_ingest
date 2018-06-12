FROM python:3.5

MAINTAINER Hao Xu version: 0.1.0

# RUN pip install rq-scheduler==0.8.2
RUN pip install git+git://github.com/as3445/rq-scheduler-bcfg.git@879d8d81d0d658c1233a0f6a4322a798f981e448

ENTRYPOINT ["rqscheduler"]
CMD ["-i", "1"]