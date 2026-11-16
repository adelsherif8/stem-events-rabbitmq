#!/usr/bin/env bash
# Run the integration tests against a throwaway RabbitMQ in Docker.
# Exits 0 with a message (does not fail) when Docker is not available.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not available (docker info failed): skipping integration tests."
  exit 0
fi

docker compose up -d rabbitmq
echo -n "waiting for RabbitMQ"
for _ in $(seq 1 60); do
  if docker compose exec -T rabbitmq rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
    echo " ready"; break
  fi
  echo -n "."; sleep 2
done

REQUIRE_RABBITMQ=1 RABBITMQ_URL=amqp://stem:stem@localhost:5672/ \
  .venv/bin/pytest -m integration -v "$@"
