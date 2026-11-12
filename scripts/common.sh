# Shared settings for every script in this project.  Usage:  . scripts/common.sh
# (No `set -e` here so it is also safe to source from an interactive shell.)

# Everything is installed under /Users/Shared so the paths are IDENTICAL on every Mac
# (Spark sends the Python path from the master to the workers, so it must match).
BIG=/Users/Shared/bigdata

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Pinned versions (all Java 11 compatible)
HADOOP_VER=3.3.6
SPARK_VER=3.5.9
KAFKA_VER=3.9.1
HBASE_VER=2.5.16
PY_VER=3.11

JDK_URL="https://api.adoptium.net/v3/binary/latest/11/ga/mac/aarch64/jdk/hotspot/normal/eclipse"
HADOOP_URL="https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VER/hadoop-$HADOOP_VER.tar.gz"
SPARK_URL="https://dlcdn.apache.org/spark/spark-$SPARK_VER/spark-$SPARK_VER-bin-hadoop3.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/$KAFKA_VER/kafka_2.13-$KAFKA_VER.tgz"
HBASE_URL="https://dlcdn.apache.org/hbase/$HBASE_VER/hbase-$HBASE_VER-hadoop3-bin.tar.gz"
DATASET_URL="https://github.com/MWiechmann/enron_spam_data/raw/master/enron_spam_data.zip"

# Per-machine settings (MASTER_IP, WORKER_CORES, WORKER_MEMORY) live in cluster.env
if [ -f "$PROJECT_DIR/cluster.env" ]; then . "$PROJECT_DIR/cluster.env"; fi
THIS_IP="${THIS_IP:-$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || echo 127.0.0.1)}"
MASTER_IP="${MASTER_IP:-$THIS_IP}"
WORKER_CORES="${WORKER_CORES:-2}"
WORKER_MEMORY="${WORKER_MEMORY:-2g}"
HDFS_URL="hdfs://$MASTER_IP:9000"
KAFKA_BOOTSTRAP="$MASTER_IP:9092"

export BIG MASTER_IP THIS_IP HDFS_URL KAFKA_BOOTSTRAP
export JAVA_HOME="$BIG/jdk"
export HADOOP_HOME="$BIG/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="$BIG/spark"
export KAFKA_HOME="$BIG/kafka"
export HBASE_HOME="$BIG/hbase"
export PYSPARK_PYTHON="$BIG/venv/bin/python"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$KAFKA_HOME/bin:$HBASE_HOME/bin:$BIG/venv/bin:$BIG/bin:$PATH"

log()  { printf '\033[1;36m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m OK\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m !!\033[0m %s\n' "$*"; }

# wait_port HOST PORT SECONDS
wait_port() {
  local i=0
  while ! nc -z "$1" "$2" >/dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -ge "$3" ] && return 1; sleep 1
  done
}
