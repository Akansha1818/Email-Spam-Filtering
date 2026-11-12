#!/usr/bin/env bash
# Health check of the whole cluster, run on the master.
. "$(dirname "$0")/common.sh"

check() {  # check NAME HOST PORT
  if nc -z -G 2 "$2" "$3" >/dev/null 2>&1; then ok "$1  ($2:$3)"; else warn "$1 DOWN ($2:$3)"; fi
}
echo "Java processes on this Mac:"; jps | grep -v Jps | sed 's/^/   /'
echo
check "HDFS NameNode      " "$MASTER_IP" 9000
check "Spark Master       " "$MASTER_IP" 7077
check "Kafka broker       " "$MASTER_IP" 9092
check "HBase (ZooKeeper)  " localhost 2181
check "HBase Thrift       " localhost 9090
check "Web app / API      " localhost 8000
echo
live=$(curl -s "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" | grep -o '"NumLiveDataNodes" : [0-9]*' | grep -o '[0-9]*$')
workers=$(curl -s "http://$MASTER_IP:8080/json/" | grep -o '"aliveworkers" : [0-9]*' | grep -o '[0-9]*$')
echo "   HDFS live DataNodes : ${live:-?}"
echo "   Spark alive workers : ${workers:-?}"
