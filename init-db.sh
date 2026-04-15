#!/bin/bash
# init-db.sh
# Runs once on first `docker-compose up` (when postgres-data volume is empty).
# Creates the two databases this project needs:
#   - airflow   → Airflow metadata
#   - wuzzufdb  → Job market warehouse
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE wuzzufdb;
EOSQL

echo "✔ Databases 'airflow' and 'wuzzufdb' created."
