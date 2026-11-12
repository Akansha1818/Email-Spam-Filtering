#!/usr/bin/env bash
# Starts all master services:
#   HDFS NameNode -> Spark Master -> Kafka -> (wait for a DataNode) -> HBase + Thrift
# For testing on ONE Mac, also start a DataNode + Spark Worker here:
#   LOCAL_WORKER=1 scripts/master-start.sh
set -euo pipefail
. "$(dirname "$0")/common.sh"
mkdir -p "$BIG"/{data/hdfs,data/kafka,logs/hadoop,logs/spark,logs/kafka,logs/hbase,run}

# Re-render configs in case the IP changed
"$PROJECT_DIR/scripts/configure.sh" master >/dev/null

# ---------- HDFS NameNode ----------
if [ ! -d "$BIG/data/hdfs/name/current" ]; then
  log "formatting HDFS NameNode (first run only)"
  hdfs namenode -format -nonInteractive -force -clusterId spam-cluster >"$BIG/logs/hadoop/format.log" 2>&1
fi
log "starting HDFS NameNode"
hdfs --daemon start namenode || true
if [ "${LOCAL_WORKER:-0}" = "1" ]; then
  log "starting local DataNode (single-Mac test mode)"
  hdfs --daemon start datanode || true
fi

# ---------- Spark standalone Master ----------
log "starting Spark Master  (spark://$MASTER_IP:7077, UI :8080)"
"$SPARK_HOME/sbin/start-master.sh" >/dev/null || true
if [ "${LOCAL_WORKER:-0}" = "1" ]; then
  "$SPARK_HOME/sbin/start-worker.sh" "spark://$MASTER_IP:7077" >/dev/null || true
fi

# ---------- Kafka (KRaft, single broker) ----------
if [ ! -f "$BIG/data/kafka/meta.properties" ]; then
  log "formatting Kafka storage (first run only)"
  kafka-storage.sh format -t "$(kafka-storage.sh random-uuid)" -c "$BIG/conf/kafka.properties" >/dev/null
fi
log "starting Kafka broker  ($KAFKA_BOOTSTRAP)"
if ! nc -z localhost 9092 2>/dev/null; then
  LOG_DIR="$BIG/logs/kafka" KAFKA_HEAP_OPTS="-Xmx512M -Xms256M" \
    kafka-server-start.sh -daemon "$BIG/conf/kafka.properties"
fi

# ---------- Wait for a DataNode (HBase stores its files in HDFS) ----------
log "waiting for at least one DataNode to join HDFS (start the workers now)..."
for i in $(seq 1 180); do
  live=$(curl -s "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" \
         | grep -o '"NumLiveDataNodes" : [0-9]*' | grep -o '[0-9]*$' || true)
  [ "${live:-0}" -ge 1 ] && break
  sleep 2
done
if [ "${live:-0}" -lt 1 ]; then
  warn "no DataNode joined yet. Start workers (scripts/worker-start.sh) and re-run this script."
  exit 1
fi
ok "$live DataNode(s) live"
hdfs dfsadmin -safemode leave >/dev/null 2>&1 || true

# ---------- HBase + Thrift gateway ----------
log "starting HBase (UI :16010) and Thrift server (:9090)"
if ! nc -z localhost 16010 2>/dev/null; then "$HBASE_HOME/bin/start-hbase.sh" >/dev/null; fi
if ! nc -z localhost 9090 2>/dev/null; then "$HBASE_HOME/bin/hbase-daemon.sh" start thrift -p 9090 >/dev/null; fi
wait_port localhost 9090 60 || warn "Thrift not up yet - check $BIG/logs/hbase"

echo
ok "master services started. Check with: scripts/status.sh"
echo "   NameNode UI : http://$MASTER_IP:9870"
echo "   Spark UI    : http://$MASTER_IP:8080"
echo "   HBase UI    : http://$MASTER_IP:16010"
