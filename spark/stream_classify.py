"""Streaming job: classifies emails in real time.

  Kafka topic `emails`  ->  Spark Structured Streaming (micro-batch every 2 s)
     -> same cleaning + trained model loaded from HDFS
     -> HBase table `emails` (one row per email) + counters in `meta`
     -> HDFS /data/stream_archive (Parquet copy of everything classified)

The Kafka offsets are check-pointed in HDFS, so if this job is stopped and
restarted it continues exactly where it left off (no email is lost).
Run with scripts/run-stream.sh
"""
import argparse
import time
from datetime import datetime

import happybase
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F, types as T

from common import with_clean_text

SCHEMA = T.StructType([
    T.StructField("rowkey", T.StringType()),
    T.StructField("id", T.StringType()),
    T.StructField("subject", T.StringType()),
    T.StructField("body", T.StringType()),
    T.StructField("sender", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("true_label", T.IntegerType()),
    T.StructField("sent_at", T.LongType()),
])


def make_writer(hbase_host, archive_path, model_name):
    def write_batch(batch_df, batch_id):
        batch_df.persist()
        rows = batch_df.collect()
        if not rows:
            batch_df.unpersist()
            return
        now = int(time.time() * 1000)

        # 1) Archive to HDFS (Parquet)
        batch_df.drop("body").withColumn("batch_id", F.lit(batch_id)) \
            .write.mode("append").parquet(archive_path)

        # 2) Store every prediction in HBase + update counters
        conn = happybase.Connection(hbase_host, port=9090, timeout=30000)
        emails, meta = conn.table("emails"), conn.table("meta")
        spam = labelled = correct = 0
        minute = {}
        with emails.batch(batch_size=500) as b:
            for r in rows:
                p_spam = float(r["p_spam"])
                is_spam = r["label"] == "spam"
                spam += is_spam
                conf = p_spam if is_spam else 1.0 - p_spam
                if r["true_label"] is not None:
                    labelled += 1
                    correct += int(r["true_label"] == int(is_spam))
                bucket = datetime.fromtimestamp((r["sent_at"] or now) / 1000).strftime("%Y%m%d%H%M")
                minute.setdefault(bucket, [0, 0])[0 if is_spam else 1] += 1
                b.put(r["rowkey"].encode(), {k.encode(): str(v).encode("utf-8") for k, v in {
                    "d:id": r["id"] or "",
                    "d:subject": (r["subject"] or "")[:300],
                    "d:body": (r["body"] or "")[:3000],
                    "d:sender": r["sender"] or "",
                    "d:source": r["source"] or "",
                    "p:label": r["label"],
                    "p:p_spam": f"{p_spam:.6f}",
                    "p:confidence": f"{conf:.6f}",
                    "p:true_label": "" if r["true_label"] is None else r["true_label"],
                    "p:sent_at": r["sent_at"] or now,
                    "p:processed_at": now,
                    "p:latency_ms": now - (r["sent_at"] or now),
                    "p:model": model_name,
                    "p:batch_id": batch_id,
                    "p:kafka_partition": r["partition"],
                    "p:kafka_offset": r["offset"],
                }.items()})

        n = len(rows)
        for col, val in [("total", n), ("spam", spam), ("ham", n - spam),
                         ("labelled", labelled), ("correct", correct)]:
            if val:
                meta.counter_inc(b"stats", f"c:{col}".encode(), val)
        for bucket, (s, h) in minute.items():
            if s:
                meta.counter_inc(f"min_{bucket}".encode(), b"c:spam", s)
            if h:
                meta.counter_inc(f"min_{bucket}".encode(), b"c:ham", h)
        conn.close()
        batch_df.unpersist()
        acc = f"  live accuracy {correct / labelled:.1%}" if labelled else ""
        print(f"[batch {batch_id}] {n} emails -> {spam} spam / {n - spam} ham{acc}", flush=True)

    return write_batch


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hdfs", default="hdfs://localhost:9000")
    ap.add_argument("--kafka", default="localhost:9092")
    ap.add_argument("--hbase", default="127.0.0.1")
    ap.add_argument("--topic", default="emails")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("SpamStreamingClassifier").getOrCreate()
    model = PipelineModel.load(f"{args.hdfs}/models/spam_model")
    model_name = type(model.stages[-1]).__name__.replace("Model", "")
    print(f"Loaded {model_name} model from HDFS; listening on Kafka topic '{args.topic}'", flush=True)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.kafka)
           .option("subscribe", args.topic)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 2000)
           .option("failOnDataLoss", "false")
           .load())

    emails = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"),
                         "partition", "offset")
              .select("e.*", "partition", "offset")
              .filter(F.col("rowkey").isNotNull()))

    scored = model.transform(with_clean_text(emails))
    out = scored.select(
        "rowkey", "id", "subject", "body", "sender", "source", "true_label", "sent_at",
        "partition", "offset",
        F.when(F.col("prediction") == 1.0, "spam").otherwise("ham").alias("label"),
        vector_to_array("probability")[1].alias("p_spam"),
    )

    query = (out.writeStream
             .foreachBatch(make_writer(args.hbase, f"{args.hdfs}/data/stream_archive", model_name))
             .option("checkpointLocation", f"{args.hdfs}/checkpoints/stream")
             .trigger(processingTime="2 seconds")
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
