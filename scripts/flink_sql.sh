#!/usr/bin/env bash
# Open the Flink SQL client with the tables from flink/sql/00_tables.sql already created.
set -euo pipefail
docker exec -it flink-jobmanager ./bin/sql-client.sh -i /opt/flink/sql/00_tables.sql