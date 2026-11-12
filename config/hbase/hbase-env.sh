# Rendered by scripts/configure.sh -> $HBASE_HOME/conf/hbase-env.sh
export JAVA_HOME=@BIG@/jdk
export HBASE_HEAPSIZE=1G
export HBASE_MANAGES_ZK=true
export HBASE_LOG_DIR=@BIG@/logs/hbase
export HBASE_PID_DIR=@BIG@/run
# The Thrift gateway (used by Python / happybase) only needs a small heap
export HBASE_THRIFT_OPTS="-Xmx256m"
export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true
export HBASE_OPTS="$HBASE_OPTS -Djava.net.preferIPv4Stack=true"
