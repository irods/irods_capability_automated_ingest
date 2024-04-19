#
# iRODS Provider Image.
#
FROM ubuntu:22.04
ARG irods_version
ARG irods_version_major_minor
ARG py_version
ENV PY_VERSION="${py_version}"

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y \
        apt-transport-https \
        gnupg \
        wget \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/*

# Install pre-requisites
RUN wget -qO - https://packages.irods.org/irods-signing-key.asc | apt-key add - && \
    echo "deb [arch=amd64] https://packages.irods.org/apt/ jammy main" | tee /etc/apt/sources.list.d/renci-irods.list

RUN apt-get update && \
    apt-get install -y \
        libcurl4-gnutls-dev \
        python3 \
        python3-distro \
        python3-jsonschema \
        python3-pip \
        python3-psutil \
        python3-requests \
        rsyslog \
        unixodbc \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/*

RUN wget -qO - https://packages.irods.org/irods-signing-key.asc | apt-key add -; \
    echo "deb [arch=amd64] https://packages.irods.org/apt/ $(lsb_release -sc) main" | tee /etc/apt/sources.list.d/renci-irods.list; \
    apt-get update && \
    apt-get install -y \
        irods-runtime=${irods_version} \
	    irods-icommands=${irods_version} \
	    irods-server=${irods_version} \
	    irods-database-plugin-postgres=${irods_version}

# Set command to execute when launching the container.
COPY --chmod=755 start_provider.sh /
COPY irods_${irods_version_major_minor}_provider.input /irods_provider.input
ENTRYPOINT ["./start_provider.sh"]
