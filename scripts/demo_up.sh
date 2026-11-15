#!/usr/bin/env bash
# One command to bring the whole pipeline up for a demo:
# Kafka, PostgreSQL, Debezium, Flink, the CDC connector and the Flink-to-Iceberg job.
# Afterwards, run the simulator and the dashboard (printed at the end).
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p ~/streaming-warehouse && chmod 777 ~/streaming-warehouse

./scripts/flink_up.sh

echo "Waiting for Kafka Connect (plugin scanning takes 2-4 minutes) ..."
until curl -sf http://localhost:8083/ > /dev/null; do sleep 5; done
./scripts/register_connector.sh > /dev/null
echo "Debezium connector registered."

echo "Submitting the Flink job (CDC -> Iceberg) ..."
docker exec -u flink flink-jobmanager ./bin/sql-client.sh \
  -i /opt/flink/sql/20_lake_tables.sql -f /opt/flink/sql/21_lake_job.sql

cat <<'MSG'

Pipeline is up. In two more terminals (with .venv active):
  python app/shop_simulator.py --n 0
  streamlit run app/lake_dashboard.py     -> http://localhost:8501
Flink UI: http://localhost:8091
MSG
