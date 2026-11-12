#!/usr/bin/env bash
# Wipes ALL cluster data on the MASTER (HDFS metadata, HBase/ZooKeeper, Kafka, checkpoints)
# so you can start clean - e.g. when switching from single-Mac test mode to the real
# 3-Mac cluster. Installed software and downloads are kept.
#   scripts/reset-cluster.sh            (asks for confirmation)
# On each WORKER, also run:  rm -rf /Users/Shared/bigdata/data/hdfs
set -euo pipefail
. "$(dirname "$0")/common.sh"
read -r -p "This deletes all HDFS/HBase/Kafka data on this Mac. Type 'yes' to continue: " ans
[ "$ans" = "yes" ] || { echo "cancelled"; exit 1; }
"$PROJECT_DIR/scripts/master-stop.sh"
rm -rf "$BIG/data/hdfs" "$BIG/data/kafka" "$BIG/data/zookeeper" "$BIG/data/hbase-tmp" \
       "$BIG/data/tmp" "$BIG/data/spark-work"
ok "data wiped. Next: scripts/master-start.sh, start the workers, scripts/init-cluster.sh, scripts/run-train.sh"
