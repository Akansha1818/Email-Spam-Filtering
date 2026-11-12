#!/usr/bin/env bash
# Streams labelled emails from the dataset into Kafka.
#   scripts/run-producer.sh --rate 20 --count 500
. "$(dirname "$0")/common.sh"
cd "$PROJECT_DIR"
exec "$BIG/venv/bin/python" producer/producer.py --kafka "$KAFKA_BOOTSTRAP" "$@"
