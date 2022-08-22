#
# iRODS Provider Image.
#
FROM ubuntu:18.04
ARG irods_version
ARG irods_version_major_minor
ARG py_version
ENV PY_VERSION="${py_version}"

# Install pre-requisites
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y sudo wget lsb-release apt-transport-https python${py_version}-pip libfuse2 unixodbc rsyslog netcat gnupg && \
    pip${py_version} install xmlrunner

RUN pip${py_version} install distro

RUN wget -qO - https://packages.irods.org/irods-signing-key.asc | apt-key add -; \
    echo "deb [arch=amd64] https://packages.irods.org/apt/ $(lsb_release -sc) main" | tee /etc/apt/sources.list.d/renci-irods.list; \
    apt-get update && \
    apt-get install -y \
        irods-runtime=${irods_version} \
	irods-icommands=${irods_version} \
	irods-server=${irods_version} \
	irods-database-plugin-postgres=${irods_version}

EXPOSE 1248 1247

# Set command to execute when launching the container.
COPY start_provider.sh /
COPY irods_${irods_version_major_minor}_provider.input /irods_provider.input
RUN chmod u+x /start_provider.sh
ENTRYPOINT ["./start_provider.sh"]
