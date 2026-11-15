#!/usr/bin/env bash
# Start Kafka and Kafka UI (http://localhost:8090).
set -euo pipefail

cd "$(dirname "$0")/../docker"
docker compose -p streaming -f docker-compose.kafka.yml up -d
docker compose -p streaming -f docker-compose.kafka.yml ps