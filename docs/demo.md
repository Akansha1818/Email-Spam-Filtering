# Demonstration script (~10 minutes)

**Before the demo:** all 3 Macs on the same hotspot, `caffeinate -dims` running on each,
`scripts/status.sh` all OK, model trained, streaming job running, dashboard open.
Have these terminals ready on the master: *(1)* commands, *(2)* streaming job, *(3)* web app.

## 1. The cluster is real (2 min)
- On each Mac run `jps`. Master: NameNode, Master, Kafka, HMaster, ThriftServer. Workers: DataNode, Worker.
- Open `http://<MASTER_IP>:9870` → *Datanodes*: **2 live DataNodes** (worker1 and worker2).
- Open `http://<MASTER_IP>:8080`: **2 Spark Workers** with their cores and memory.
- Dashboard → **Cluster** tab: the same information, read live through REST/JMX.

## 2. HDFS: distributed, replicated storage (1.5 min)
```bash
hdfs dfs -ls -h /data /models
hdfs fsck /data/emails.jsonl -files -blocks -locations
```
Point out that the 51 MB file is split into **4 blocks of 16 MB**, and each block has
**2 replicas on 2 different DataNodes**.

## 3. Distributed training with Spark MLlib (2 min)
```bash
scripts/run-train.sh
```
While it runs, open `http://<MASTER_IP>:4040` → *Executors* and show tasks running on **both workers**.
The output prints the Naive Bayes vs Logistic Regression comparison (≈ 97.8 % accuracy).
Then show the dashboard → **Analytics** tab: metrics, confusion matrix, spam words.
*(If the streaming job is running, stop it with Ctrl+C before training and restart it afterwards.)*

## 4. Real-time streaming (2 min)
```bash
scripts/run-stream.sh                                    # terminal 2
scripts/run-producer.sh --rate 30 --count 600            # terminal 1
kafka-console-consumer.sh --bootstrap-server <MASTER_IP>:9092 --topic emails --max-messages 2
```
Show the dashboard → **Live Feed**: rows appear every 2 s, counters rise, live accuracy is ≈ 98 %.
The streaming terminal prints `[batch N] … emails -> … spam / … ham`.

## 5. The examiner tries it (1.5 min)
Dashboard → **Classify**: let the examiner type any email, or use a sample button.
The result shows the label, confidence and latency, plus the email's path:
Kafka partition/offset → Spark micro-batch → HBase row key. Prove it is stored:
```bash
hbase shell
get 'emails', '<row key copied from the dashboard>'
count 'emails'
get_counter 'meta', 'stats', 'c:spam'
```

## 6. Fault tolerance (1 min)
1. Stop the streaming job with **Ctrl+C**.
2. Send emails while it is down: `scripts/run-producer.sh --rate 50 --count 100`. They wait in Kafka.
3. Restart `scripts/run-stream.sh`. It resumes from the **Kafka offsets checkpointed in HDFS** and
   processes exactly those 100 emails. The total in the dashboard rises by 100, so nothing was lost.

(Optional: stop one worker's DataNode with `hdfs --daemon stop datanode`. `hdfs dfs -cat` still works
because each block has a second replica.)

## Viva questions to be ready for
- **Why Kafka?** It decouples producers from Spark, absorbs bursts, keeps messages while Spark is down, and its partitions allow parallel reads.
- **Why HBase rather than MySQL?** It does fast random reads and writes by row key, scales horizontally, stores its data in HDFS, and has atomic counters.
- **Why the same `common.py` for training and streaming?** It avoids training/serving skew: text is cleaned identically in both.
- **Why HashingTF?** It needs no vocabulary to be stored, uses fixed memory and works on unseen words in a stream.
- **Why Naive Bayes?** It is fast, strong on text, and gives probabilities. Logistic Regression is compared alongside.
- **Is 33k emails "big data"?** No. The point is that the architecture scales horizontally: more workers, more partitions and more brokers, with no code changes.
