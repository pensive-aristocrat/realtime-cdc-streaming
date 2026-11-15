#!/usr/bin/env bash
# Open the Flink SQL client with a table-definition file already run.
# Usage: ./scripts/flink_sql.sh                        (uses 00_tables.sql, Module 18)
#        ./scripts/flink_sql.sh 10_payments_tables.sql (Module 19)
set -euo pipefail
INIT_FILE="${1:-00_tables.sql}"
docker exec -it flink-jobmanager ./bin/sql-client.sh -i "/opt/flink/sql/${INIT_FILE}"