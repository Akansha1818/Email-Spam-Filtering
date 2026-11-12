#!/usr/bin/env bash
# Trains the spam model on the Spark cluster (reads HDFS, writes model to HDFS, metrics to HBase).
#   scripts/run-train.sh            # deploy Naive Bayes (default)
#   scripts/run-train.sh --deploy lr
set -euo pipefail
. "$(dirname "$0")/common.sh"
cd "$PROJECT_DIR"
spark-submit \
  --master "spark://$MASTER_IP:7077" \
  --name SpamModelTraining \
  --total-executor-cores 6 \
  --executor-memory 1g \
  --py-files spark/common.py \
  spark/train_model.py --hdfs "$HDFS_URL" "$@"
