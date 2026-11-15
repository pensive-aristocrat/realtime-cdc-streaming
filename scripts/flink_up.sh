#!/usr/bin/env bash
# Start only what the Flink modules need: Kafka, the shop database, Debezium and Flink.
# (Schema Registry and Kafka UI stay off to save memory; start them with cdc_up.sh when needed.)
set -euo pipefail

cd "$(dirname "$0")/../docker"
COMPOSE="docker compose -p streaming -f docker-compose.kafka.yml -f docker-compose.cdc.yml -f docker-compose.flink.yml"
$COMPOSE up -d --build kafka postgres connect jobmanager taskmanager

echo "Waiting for Flink on http://localhost:8091 ..."
until curl -sf http://localhost:8091/overview > /dev/null; do sleep 3; done
curl -s http://localhost:8091/overview; echo