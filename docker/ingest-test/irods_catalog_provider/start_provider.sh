#! /bin/bash

# Start the Postgres database.
counter=0
until pg_isready -h irods-catalog -d ICAT -U irods -q
do
    sleep 1
    ((counter += 1))
done
echo Postgres took approximately $counter seconds to fully start ...

# Set up iRODS if not already done
if [ ! -e /var/lib/irods/setup_complete ]
    then
        python3 /var/lib/irods/scripts/setup_irods.py < /irods_provider.input
fi

# run the server
su - irods -c "/var/lib/irods/irodsctl restart"

touch /var/lib/irods/setup_complete

# Keep container running if the test fails.
tail -f /dev/null
# Is this better? sleep 2147483647d

