#!/usr/bin/env bash
# Starts the real-time classifier: Kafka -> Spark Structured Streaming -> HBase (+ HDFS archive).
# Runs until Ctrl+C. The Kafka connector JARs are downloaded on first run (needs internet once).
set -euo pipefail
. "$(dirname "$0")/common.sh"
cd "$PROJECT_DIR"
spark-submit \
  --master "spark://$MASTER_IP:7077" \
  --name SpamStreamingClassifier \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:$SPARK_VER" \
  --total-executor-cores 4 \
  --executor-memory 1g \
  --py-files spark/common.py \
  spark/stream_classify.py --hdfs "$HDFS_URL" --kafka "$KAFKA_BOOTSTRAP" "$@"
