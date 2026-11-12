#!/usr/bin/env bash
# Run on each WORKER Mac.
. "$(dirname "$0")/common.sh"
"$SPARK_HOME/sbin/stop-worker.sh" >/dev/null 2>&1 || true
hdfs --daemon stop datanode >/dev/null 2>&1 || true
ok "worker stopped"
