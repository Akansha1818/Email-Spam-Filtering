#!/usr/bin/env bash
# One-time (idempotent) setup on the MASTER after the cluster is running:
# HDFS folders + dataset upload, Kafka topic, HBase tables.
set -euo pipefail
. "$(dirname "$0")/common.sh"

log "HDFS: creating folders"
hdfs dfs -mkdir -p /data /models /checkpoints

if hdfs dfs -test -e /data/emails.jsonl; then
  ok "dataset already in HDFS"
else
  log "HDFS: uploading dataset with a 16 MB block size (so blocks spread over both workers)"
  hdfs dfs -D dfs.blocksize=16777216 -put "$PROJECT_DIR/data/emails.jsonl" /data/
fi
hdfs dfs -ls -h /data

log "Kafka: creating topic 'emails' (2 partitions)"
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --create --if-not-exists \
  --topic emails --partitions 2 --replication-factor 1
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic emails

log "HBase: creating tables"
"$BIG/venv/bin/python" "$PROJECT_DIR/scripts/create_tables.py"
ok "cluster initialised"
