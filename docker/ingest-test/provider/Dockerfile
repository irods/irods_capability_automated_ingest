#
# iRODS Provider Image.
#
FROM ubuntu:18.04

# Install pre-requisites
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y sudo wget lsb-release apt-transport-https python-pip libfuse2 unixodbc rsyslog netcat gnupg && \
    pip install xmlrunner

RUN wget -qO - https://packages.irods.org/irods-signing-key.asc | apt-key add -; \
    echo "deb [arch=amd64] https://packages.irods.org/apt/ $(lsb_release -sc) main" | tee /etc/apt/sources.list.d/renci-irods.list; \
    apt-get update && \
    apt-get install -y irods-runtime irods-icommands irods-server irods-database-plugin-postgres

EXPOSE 1248 1247

# Set command to execute when launching the container.
COPY start_provider.sh irods_provider.input /
RUN chmod u+x /start_provider.sh
ENTRYPOINT ["./start_provider.sh"]
