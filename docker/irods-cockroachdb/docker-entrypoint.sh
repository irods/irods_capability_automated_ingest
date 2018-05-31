#!/usr/bin/env bash
set -e

export COCKROACH_HOST=${IRODS_DATABASE_SERVER_HOSTNAME}
export COCKROACH_PORT=${IRODS_DATABASE_SERVER_PORT}
export COCKROACH_INSECURE=1

### update UID:GID values for irods-postgresql service account
_update_uid_gid() {
  # update UID
  usermod -u ${UID_IRODS} irods
  # update GID
  groupmod -g ${GID_IRODS} irods
  # update directories
  chown -R irods:irods /var/lib/irods
  chown -R irods:irods /etc/irods
}

### initialize ICAT database
_database_setup() {
  cockroach sql -e "CREATE USER ${IRODS_DATABASE_USER_NAME}; CREATE DATABASE \"${IRODS_DATABASE_NAME}\"; GRANT ALL ON DATABASE \"${IRODS_DATABASE_NAME}\" TO ${IRODS_DATABASE_USER_NAME};"
}

### populate contents of /var/lib/irods if external volume mount is used
_irods_tgz() {
  cp /irods.tar.gz /var/lib/irods/irods.tar.gz
  cd /var/lib/irods/
  echo "!!! populating /var/lib/irods with initial contents !!!"
  tar -zxvf irods.tar.gz
  cd /
  rm -f /var/lib/irods/irods.tar.gz
}

### populate contents of /var/lib/postgresql/data if external volume mount is used
_postgresql_tgz() {
  cp /postgresql.tar.gz /var/lib/postgresql/data/postgresql.tar.gz
  cd /var/lib/postgresql/data
  echo "!!! populating /var/lib/postgresql/data with initial contents !!!"
  tar -zxvf postgresql.tar.gz
  cd /
  rm -f /var/lib/postgresql/data/postgresql.tar.gz
}

### generate iRODS config file
_generate_config() {
    cat > /irods.config <<EOF
${IRODS_SERVICE_ACCOUNT_NAME}
${IRODS_SERVICE_ACCOUNT_GROUP}
${IRODS_SERVER_ROLE}
${ODBC_DRIVER_FOR_POSTGRES}
${IRODS_DATABASE_SERVER_HOSTNAME}
${IRODS_DATABASE_SERVER_PORT}
${IRODS_DATABASE_NAME}
${IRODS_DATABASE_USER_NAME}
yes
${IRODS_DATABASE_PASSWORD}
${IRODS_DATABASE_USER_PASSWORD_SALT}
${IRODS_ZONE_NAME}
${IRODS_PORT}
${IRODS_PORT_RANGE_BEGIN}
${IRODS_PORT_RANGE_END}
${IRODS_CONTROL_PLANE_PORT}
${IRODS_SCHEMA_VALIDATION}
${IRODS_SERVER_ADMINISTRATOR_USER_NAME}
yes
${IRODS_SERVER_ZONE_KEY}
${IRODS_SERVER_NEGOTIATION_KEY}
${IRODS_CONTROL_PLANE_KEY}
${IRODS_SERVER_ADMINISTRATOR_PASSWORD}
${IRODS_VAULT_DIRECTORY}
EOF
}

### update hostname if it has changed across docker containers using volume mounts
_hostname_update() {
  local EXPECTED_HOSTNAME=$(sed -e 's/^"//' -e 's/"//' \
    <<<$(cat /var/lib/irods/.irods/irods_environment.json | \
    jq .irods_host))
  if [[ "${EXPECTED_HOSTNAME}" != $(hostname) ]]; then
    echo "### Updating hostname ###"
    jq '.irods_host = "'$(hostname)'"' \
      /var/lib/irods/.irods/irods_environment.json|sponge \
      /vaSERVICE_r/lib/irods/.irods/irods_environment.json
    jq '.catalog_provider_hosts[] = "'$(hostname)'"' \
      /etc/irods/server_config.json|sponge \
      /etc/irods/server_config.json
    echo  > update_hostname.sql
    cockroach sql -d ICAT -u '${IRODS_DATABASE_USER_NAME}' -e "UPDATE r_resc_main SET resc_net = '"$(hostname)"' WHERE resc_net = '${EXPECTED_HOSTNAME}';"
  fi
}

### main ###

### external volume mounts will be empty on initial run
if [[ ! -f /var/lib/irods/irodsctl ]]; then
  _irods_tgz
fi

### check for UID:GID change and update directory permissions
_update_uid_gid

### wait for database to stand up
until [ $(pg_isready -h ${IRODS_DATABASE_SERVER_HOSTNAME} -p ${IRODS_DATABASE_SERVER_PORT} -q)$? -eq 0 ]; do
  echo "pg_isready -h ${IRODS_DATABASE_SERVER_HOSTNAME} -p ${IRODS_DATABASE_SERVER_PORT} -q"
  echo "CockroachDB is unavailable - sleeping"
  sleep 2
done

echo $1
### start iRODS
if [[ $1 == "init" ]]; then
  ### initialize iRODS
  _database_setup
  _generate_config
  python /var/lib/irods/scripts/setup_irods.py < /irods.config
  _update_uid_gid
else
  _hostname_update
  service irods start
  su irods -c 'echo ${IRODS_SERVER_ADMINISTRATOR_PASSWORD} | iinit'
  ### Keep a foreground process running forever
  tail -f /dev/null
fi


exit 0;
