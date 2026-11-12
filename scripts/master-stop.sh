#!/usr/bin/env bash
# Stops all master services (reverse order).
. "$(dirname "$0")/common.sh"
log "stopping web app";        pkill -f "uvicorn backend.app:app" 2>/dev/null || true
log "stopping Spark jobs";     pkill -f "spark/stream_classify.py" 2>/dev/null || true
log "stopping HBase Thrift";   "$HBASE_HOME/bin/hbase-daemon.sh" stop thrift >/dev/null 2>&1 || true
                               pkill -f "org.apache.hadoop.hbase.thrift.ThriftServer" 2>/dev/null || true
log "stopping HBase";          "$HBASE_HOME/bin/stop-hbase.sh" >/dev/null 2>&1 || true
log "stopping Kafka";          kafka-server-stop.sh >/dev/null 2>&1 || true
log "stopping Spark";          "$SPARK_HOME/sbin/stop-worker.sh" >/dev/null 2>&1 || true
                               "$SPARK_HOME/sbin/stop-master.sh" >/dev/null 2>&1 || true
log "stopping HDFS";           hdfs --daemon stop datanode >/dev/null 2>&1 || true
                               hdfs --daemon stop namenode >/dev/null 2>&1 || true
ok "master stopped"
