#!/usr/bin/env bash
# Create the Debezium connector, or update it if it already exists (PUT is safe to rerun).
set -euo pipefail

cd "$(dirname "$0")/.."
curl -s -X PUT -H "Content-Type: application/json" \
  --data @connectors/shop-postgres.json \
  http://localhost:8083/connectors/shop-postgres/config | python3 -m json.tool
sleep 5
curl -s http://localhost:8083/connectors/shop-postgres/status | python3 -m json.tool