FROM krallin/ubuntu-tini:xenial
MAINTAINER Michael J. Stealey <michael.j.stealey@gmail.com>
MAINTAINER Hao Xu <xuhao@renci.org>

ENV IRODS_VERSION=4.2.2

# set user/group IDs for irods-postgresql account
RUN groupadd -r irods --gid=998 \
    && useradd -r -g irods -d /var/lib/irods --uid=998 irods

# install iRODS 4.2.2
RUN apt-get update && apt-get install -y \
  wget \
  gnupg2 \
  apt-transport-https \
  sudo \
  jq \
  libxml2 \
  moreutils \
  postgresql-client \
  && wget -qO - https://packages.irods.org/irods-signing-key.asc | apt-key add - \
  && echo "deb [arch=amd64] https://packages.irods.org/apt/ xenial main" \
  > /etc/apt/sources.list.d/renci-irods.list \
  && apt-get update && apt-get install -y \
  irods-database-plugin-postgres=${IRODS_VERSION} \
  irods-icommands=${IRODS_VERSION}  \
  irods-runtime=${IRODS_VERSION}  \
  irods-server=${IRODS_VERSION}

# default iRODS and PostgreSQL environment variables
ENV IRODS_SERVICE_ACCOUNT_NAME=irods \
  IRODS_SERVICE_ACCOUNT_GROUP=irods \
  IRODS_SERVER_ROLE=1 \
  ODBC_DRIVER_FOR_POSTGRES=2 \
  IRODS_DATABASE_SERVER_HOSTNAME=localhost \
  IRODS_DATABASE_SERVER_PORT=5432 \
  IRODS_DATABASE_NAME=ICAT \
  IRODS_DATABASE_USER_NAME=irods \
  IRODS_DATABASE_PASSWORD=temppassword \
  IRODS_DATABASE_USER_PASSWORD_SALT=tempsalt \
  IRODS_ZONE_NAME=tempZone \
  IRODS_PORT=1247 \
  IRODS_PORT_RANGE_BEGIN=20000 \
  IRODS_PORT_RANGE_END=20199 \
  IRODS_CONTROL_PLANE_PORT=1248 \
  IRODS_SCHEMA_VALIDATION=file:///var/lib/irods/configuration_schemas \
  IRODS_SERVER_ADMINISTRATOR_USER_NAME=rods \
  IRODS_SERVER_ZONE_KEY=TEMPORARY_zone_key \
  IRODS_SERVER_NEGOTIATION_KEY=TEMPORARY_32byte_negotiation_key \
  IRODS_CONTROL_PLANE_KEY=TEMPORARY__32byte_ctrl_plane_key \
  IRODS_SERVER_ADMINISTRATOR_PASSWORD=rods \
  IRODS_VAULT_DIRECTORY=/var/lib/irods/iRODS/Vault \
  UID_IRODS=998 \
  GID_IRODS=998

# create irods-postgresql.tar.gz
RUN cd /var/lib/irods \
    && tar -czvf /irods.tar.gz . \
    && cd /

COPY ./docker-entrypoint.sh /irods-docker-entrypoint.sh
VOLUME /var/lib/irods /etc/irods /var/lib/postgresql/data

EXPOSE $IRODS_PORT $IRODS_CONTROL_PLANE_PORT $IRODS_PORT_RANGE_BEGIN-$IRODS_PORT_RANGE_END

WORKDIR /var/lib/irods/
ENTRYPOINT ["/irods-docker-entrypoint.sh"]
