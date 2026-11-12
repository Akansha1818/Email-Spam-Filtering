#!/usr/bin/env bash
# Renders config/ templates into the installed tools for THIS Mac.
# Re-run it whenever an IP address changes (e.g. after reconnecting to the hotspot).
#   scripts/configure.sh master|worker
set -euo pipefail
ROLE="${1:-worker}"
. "$(dirname "$0")/common.sh"

render() {  # render TEMPLATE DEST
  sed -e "s|@MASTER_IP@|$MASTER_IP|g" -e "s|@THIS_IP@|$THIS_IP|g" -e "s|@BIG@|$BIG|g" \
      -e "s|@WORKER_CORES@|$WORKER_CORES|g" -e "s|@WORKER_MEMORY@|$WORKER_MEMORY|g" \
      "$PROJECT_DIR/config/$1" > "$2"
}

C="$HADOOP_HOME/etc/hadoop"
render hadoop/core-site.xml "$C/core-site.xml"
render hadoop/hdfs-site.xml "$C/hdfs-site.xml"
render hadoop/hadoop-env.sh "$C/hadoop-env.sh"

render spark/spark-env.sh "$SPARK_HOME/conf/spark-env.sh"
render spark/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"
# Quieter Spark console output (WARN instead of INFO)
sed 's/^rootLogger.level = info/rootLogger.level = warn/' \
  "$SPARK_HOME/conf/log4j2.properties.template" > "$SPARK_HOME/conf/log4j2.properties"

if [ "$ROLE" = "master" ]; then
  render kafka/server.properties "$BIG/conf/kafka.properties"
  render hbase/hbase-site.xml "$HBASE_HOME/conf/hbase-site.xml"
  render hbase/hbase-env.sh "$HBASE_HOME/conf/hbase-env.sh"
fi

ok "configured $ROLE: THIS_IP=$THIS_IP MASTER_IP=$MASTER_IP"
