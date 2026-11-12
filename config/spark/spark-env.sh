# Rendered by scripts/configure.sh -> $SPARK_HOME/conf/spark-env.sh
export JAVA_HOME=@BIG@/jdk
export HADOOP_CONF_DIR=@BIG@/hadoop/etc/hadoop
export SPARK_MASTER_HOST=@MASTER_IP@
export SPARK_LOCAL_IP=@THIS_IP@
export SPARK_PUBLIC_DNS=@THIS_IP@

# Resources this machine offers when it runs a Spark Worker
export SPARK_WORKER_CORES=@WORKER_CORES@
export SPARK_WORKER_MEMORY=@WORKER_MEMORY@
export SPARK_DAEMON_MEMORY=512m
# Laptops on Wi-Fi drop out briefly: wait 3 min (default 1 min) before declaring a worker dead
export SPARK_MASTER_OPTS="-Dspark.worker.timeout=180"

export PYSPARK_PYTHON=@BIG@/venv/bin/python
export SPARK_LOG_DIR=@BIG@/logs/spark
export SPARK_PID_DIR=@BIG@/run
export SPARK_WORKER_DIR=@BIG@/data/spark-work
# Clean up finished application folders on workers
export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.appDataTtl=3600"
