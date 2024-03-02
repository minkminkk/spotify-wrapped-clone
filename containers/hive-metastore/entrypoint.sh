#!/bin/bash
set -x

: ${DB_DRIVER:=derby}

function initialize_hive {
  $HIVE_HOME/bin/schematool -dbType $DB_DRIVER -initSchema
  if [ $? -eq 0 ]; then
    echo "Initialized schema successfully.."
  else
    echo "Schema initialization failed!"
    exit 1
  fi
}

schema_version = $HIVE_HOME/bin/schematool -dbType $DB_DRIVER -info \
    | grep 'Metastore schema' \
    | awk '{ print $4 }' 

if [[ $schema_version != '3.1.0' ]]; then
    echo "Schema not found. Initializing..."
    initialize_hive
fi

exec $HIVE_HOME/bin/hive --skiphadoopversion --skiphbasecp --service $SERVICE_NAME