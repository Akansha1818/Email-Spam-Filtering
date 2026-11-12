#!/usr/bin/env bash
# Run on each WORKER Mac: joins HDFS (DataNode) and Spark (Worker).
set -euo pipefail
. "$(dirname "$0")/common.sh"
mkdir -p "$BIG"/{data/hdfs,logs/hadoop,logs/spark,run}
"$PROJECT_DIR/scripts/configure.sh" worker >/dev/null

log "checking that the master ($MASTER_IP) is reachable"
nc -z -G 3 "$MASTER_IP" 9000 || { warn "cannot reach NameNode at $MASTER_IP:9000 - is the master started and on the same Wi-Fi?"; exit 1; }
nc -z -G 3 "$MASTER_IP" 7077 || { warn "cannot reach Spark Master at $MASTER_IP:7077"; exit 1; }

log "starting HDFS DataNode"
hdfs --daemon start datanode || true
log "starting Spark Worker ($WORKER_CORES cores, $WORKER_MEMORY)"
"$SPARK_HOME/sbin/start-worker.sh" "spark://$MASTER_IP:7077" >/dev/null || true
sleep 3
jps | grep -E "DataNode|Worker" || true
ok "worker $THIS_IP started - it should now appear at http://$MASTER_IP:9870 and http://$MASTER_IP:8080"
echo "   Keep this Mac awake during the demo:  caffeinate -dims"
