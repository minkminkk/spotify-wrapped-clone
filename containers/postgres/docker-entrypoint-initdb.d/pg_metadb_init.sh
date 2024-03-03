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

# Grant permissions on schema public (PostgreSQL 15+)
psql --username postgres --dbname airflow_meta_db \
    -c "GRANT ALL ON SCHEMA public TO airflow;"
psql --username postgres --dbname hive_meta_db \
    -c "GRANT ALL ON SCHEMA public TO hive;"

# Copy SQL schema init script to postgres
echo "Executing Hive schema init SQL script..."
sql_path=/docker-entrypoint-initdb.d/sql/hive-schema-3.1.0.sql
psql -q --username hive --dbname hive_meta_db -f $sql_path

# Grant permission on tables to airflow for writing jobs
psql --username postgres --dbname hive_meta_db \
    -c "GRANT ALL ON ALL TABLES IN SCHEMA public TO airflow;"