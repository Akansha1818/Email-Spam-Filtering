# Basic commands, with explanations

First load the environment in any terminal: `source scripts/common.sh`.
That puts `hdfs`, `spark-submit`, `kafka-*.sh` and `hbase` on your PATH.

## Hadoop HDFS

| Command | What it does |
|---|---|
| `jps` | Lists running Java processes. Master: `NameNode`, `Master`, `Kafka`, `HMaster`, `ThriftServer`. Worker: `DataNode`, `Worker` |
| `hdfs dfsadmin -report` | Cluster capacity and the list of **live DataNodes** (should be 2) |
| `hdfs dfs -ls -h /` | Lists the HDFS root: `/data`, `/models`, `/checkpoints`, `/hbase` |
| `hdfs dfs -mkdir -p /demo` | Creates a folder in HDFS |
| `hdfs dfs -put data/README.md /demo/` | Copies a local file **into** HDFS |
| `hdfs dfs -cat /demo/README.md` | Prints a file stored in HDFS |
| `hdfs dfs -get /demo/README.md /tmp/x.md` | Copies a file **out of** HDFS |
| `hdfs dfs -du -h /` | Space used per folder |
| `hdfs fsck /data/emails.jsonl -files -blocks -locations` | Shows the file's **blocks** (16 MB each) and **which DataNodes hold each replica**. This proves the data is distributed and replicated (×2) |
| `hdfs dfs -ls /models/spam_model/stages` | The saved Spark ML pipeline: tokenizer, stop-words, hashingTF, IDF, NaiveBayes |
| `hdfs dfs -ls /data/stream_archive` | Parquet files written by the streaming job |
| `hdfs dfs -rm -r /demo` | Deletes a folder |
| `hdfs dfsadmin -safemode get` | Whether HDFS is in read-only safe mode |

Web UI: `http://<MASTER_IP>:9870` → *Datanodes* tab.

## Apache Spark (standalone cluster)

| Command | What it does |
|---|---|
| `$SPARK_HOME/sbin/start-master.sh` | Starts the Spark Master (cluster manager) on `spark://<MASTER_IP>:7077` |
| `$SPARK_HOME/sbin/start-worker.sh spark://<MASTER_IP>:7077` | On a worker: registers this Mac's cores and memory with the master |
| `spark-submit --master spark://<MASTER_IP>:7077 $SPARK_HOME/examples/src/main/python/pi.py 100` | Classic test job: estimates π with tasks spread over the workers |
| `scripts/run-train.sh` | `spark-submit` of `spark/train_model.py`: distributed training (`--total-executor-cores 6 --executor-memory 1g`) |
| `scripts/run-stream.sh` | `spark-submit --packages spark-sql-kafka…` of the streaming classifier |
| `pyspark --master spark://<MASTER_IP>:7077` | Interactive Spark shell on the cluster, e.g. `spark.read.json("hdfs:///data/emails.jsonl").groupBy("label").count().show()` |

Web UIs: Master `http://<MASTER_IP>:8080` (workers, apps). Running job `http://<MASTER_IP>:4040` (stages, tasks per executor, Structured Streaming tab).

## Apache Kafka (single broker, KRaft mode)

| Command | What it does |
|---|---|
| `kafka-topics.sh --bootstrap-server <MASTER_IP>:9092 --list` | Lists topics |
| `kafka-topics.sh --bootstrap-server <MASTER_IP>:9092 --describe --topic emails` | Partitions (2), leader, replicas |
| `kafka-console-producer.sh --bootstrap-server <MASTER_IP>:9092 --topic test` | Type messages to send them (Ctrl+C to exit) |
| `kafka-console-consumer.sh --bootstrap-server <MASTER_IP>:9092 --topic emails --from-beginning --max-messages 3` | Reads messages from the topic |
| `kafka-console-consumer.sh --bootstrap-server <MASTER_IP>:9092 --topic emails` | Watches new emails arrive live (run next to the producer) |
| `kafka-get-offsets.sh --bootstrap-server <MASTER_IP>:9092 --topic emails` | Latest offset (= message count) per partition |
| `kafka-storage.sh format -t <uuid> -c …` | One-time formatting of KRaft storage (done by `master-start.sh`) |

## Apache HBase

Open the shell with `hbase shell`:

| Command | What it does |
|---|---|
| `status` | Server status |
| `list` | Tables: `emails`, `meta` |
| `describe 'emails'` | Column families `d` (email data) and `p` (prediction) |
| `count 'emails'` | Number of classified emails |
| `scan 'emails', {LIMIT => 2}` | First 2 rows (newest first, thanks to the reversed-timestamp row key) |
| `scan 'emails', {COLUMNS => ['p:label','p:confidence'], LIMIT => 5}` | Only specific columns |
| `get 'emails', '<rowkey>'` | One email by row key (shown in the dashboard result) |
| `get_counter 'meta', 'stats', 'c:spam'` | Live atomic counter maintained by Spark Streaming |
| `get 'meta', 'model'` | Training metrics JSON |
| `create 'test', 'cf'` / `put 'test', 'r1', 'cf:a', 'hello'` / `get 'test', 'r1'` / `disable 'test'` / `drop 'test'` | Basic CRUD demo |

Web UI: `http://<MASTER_IP>:16010`.
