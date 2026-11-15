#!/usr/bin/env bash
# Start Kafka, Schema Registry, Kafka UI, the shop database and Kafka Connect (Debezium).
set -euo pipefail

cd "$(dirname "$0")/../docker"
COMPOSE="docker compose -p streaming -f docker-compose.kafka.yml -f docker-compose.cdc.yml"
$COMPOSE up -d

echo "Waiting for Kafka Connect on http://localhost:8083 ..."
until curl -sf http://localhost:8083/ > /dev/null; do sleep 3; done
$COMPOSE ps