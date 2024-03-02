#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username postgres --dbname postgres <<-EOSQL
	CREATE DATABASE airflow_meta_db;
    CREATE USER airflow WITH PASSWORD 'airflow';
    GRANT ALL PRIVILEGES ON DATABASE airflow_meta_db TO airflow;

    CREATE DATABASE hive_meta_db;
    CREATE USER hive WITH PASSWORD 'hive';
    GRANT ALL PRIVILEGES ON DATABASE hive_meta_db TO hive;
EOSQL

psql -v ON_ERROR_STOP=1 --username postgres --dbname airflow_meta_db \
    -c "GRANT ALL ON SCHEMA public TO airflow;" \

psql -v ON_ERROR_STOP=1 --username postgres --dbname hive_meta_db \
    -c "GRANT ALL ON SCHEMA public TO hive;"
