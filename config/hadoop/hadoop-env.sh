# Rendered by scripts/configure.sh -> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
export JAVA_HOME=@BIG@/jdk
export HADOOP_HEAPSIZE_MAX=512m
export HADOOP_LOG_DIR=@BIG@/logs/hadoop
export HADOOP_PID_DIR=@BIG@/run
export HADOOP_OPTS="$HADOOP_OPTS -Djava.net.preferIPv4Stack=true"
