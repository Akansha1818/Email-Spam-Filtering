"""Batch job: trains the spam classifier on the Spark cluster.

  HDFS /data/emails.jsonl
    -> clean text -> TF-IDF -> Naive Bayes  (+ Logistic Regression for comparison)
    -> evaluate on a 20 % hold-out set
    -> save the deployed model to HDFS /models/spam_model
    -> save metrics + dataset insights to HBase table `meta`

Run with scripts/run-train.sh
"""
import argparse
import json
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession, functions as F

from common import feature_stages, with_clean_text

ALGORITHMS = {
    "nb": ("Naive Bayes (multinomial)",
           lambda: NaiveBayes(featuresCol="features", labelCol="label",
                              modelType="multinomial", smoothing=1.0)),
    "lr": ("Logistic Regression",
           lambda: LogisticRegression(featuresCol="features", labelCol="label",
                                      maxIter=50, regParam=0.01)),
}


def evaluate(pred):
    """Confusion matrix + standard metrics (spam = positive class)."""
    cm = {(int(r["label"]), int(r["prediction"])): r["count"]
          for r in pred.groupBy("label", "prediction").count().collect()}
    tp, tn = cm.get((1, 1), 0), cm.get((0, 0), 0)
    fp, fn = cm.get((0, 1), 0), cm.get((1, 0), 0)
    total = tp + tn + fp + fn
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="probability",
                                        metricName="areaUnderROC").evaluate(pred)
    return {
        "accuracy": (tp + tn) / total if total else 0.0,
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
        "auc": auc,
        "confusion": {"tp": tp, "tn": tn, "fp": fp, "fn": fn},
        "test_rows": total,
    }


def dataset_insights(words_df):
    """Spark aggregations describing the dataset (shown in the dashboard)."""
    by_class = {int(r["label"]): r for r in words_df.groupBy("label").agg(
        F.count("*").alias("n"), F.avg(F.size("words")).alias("avg_words")).collect()}
    n_spam, n_ham = by_class.get(1)["n"], by_class.get(0)["n"]

    # Words that appear in a much larger share of spam emails than ham emails (and vice versa)
    docs = words_df.select("label", F.explode(F.array_distinct("words")).alias("word"))
    stats = (docs.groupBy("word")
             .agg(F.sum("label").alias("spam_docs"), F.count("*").alias("docs"))
             .withColumn("ham_docs", F.col("docs") - F.col("spam_docs"))
             .filter((F.length("word") >= 3) & ~F.col("word").endswith("token"))
             .withColumn("spam_rate", (F.col("spam_docs") + 1) / (n_spam + 2))
             .withColumn("ham_rate", (F.col("ham_docs") + 1) / (n_ham + 2))
             .cache())

    def top(cond, order, count_col):
        return [{"word": r["word"], "count": int(r[count_col])}
                for r in stats.filter(cond).orderBy(F.desc(order)).limit(15).collect()]

    top_spam = top(F.col("spam_docs") >= 150, F.col("spam_rate") / F.col("ham_rate"), "spam_docs")
    top_ham = top(F.col("ham_docs") >= 150, F.col("ham_rate") / F.col("spam_rate"), "ham_docs")

    # Share of emails containing links / money amounts (placeholder tokens)
    signals = {}
    for tok in ["urltoken", "moneytoken", "emailtoken"]:
        rows = words_df.groupBy("label").agg(
            F.avg(F.array_contains("tokens", tok).cast("double")).alias("share")).collect()
        signals[tok.replace("token", "")] = {("spam" if r["label"] == 1 else "ham"): round(r["share"], 4)
                                             for r in rows}

    # Email length distribution (words) per class
    bucket = (F.when(F.size("words") < 50, "<50").when(F.size("words") < 150, "50-150")
              .when(F.size("words") < 400, "150-400").otherwise("400+"))
    hist = {}
    for r in words_df.groupBy(bucket.alias("b"), "label").count().collect():
        hist.setdefault(r["b"], {"bucket": r["b"], "spam": 0, "ham": 0})[
            "spam" if r["label"] == 1 else "ham"] = r["count"]
    order = ["<50", "50-150", "150-400", "400+"]
    return {
        "total": n_spam + n_ham, "spam": n_spam, "ham": n_ham,
        "avg_words": {"spam": round(by_class[1]["avg_words"], 1), "ham": round(by_class[0]["avg_words"], 1)},
        "top_spam_words": top_spam, "top_ham_words": top_ham,
        "signals": signals,
        "length_hist": [hist[b] for b in order if b in hist],
    }


def save_to_hbase(host, rows):
    try:
        import happybase
        conn = happybase.Connection(host, port=9090, timeout=30000)
        meta = conn.table("meta")
        for row_key, payload in rows.items():
            meta.put(row_key.encode(), {b"m:json": json.dumps(payload).encode()})
        conn.close()
        print(f"saved {list(rows)} to HBase table 'meta'")
    except Exception as e:  # training is still useful without HBase
        print(f"WARNING: could not write to HBase ({e}). Run scripts/init-cluster.sh first?")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hdfs", default="hdfs://localhost:9000")
    ap.add_argument("--deploy", choices=list(ALGORITHMS), default="nb")
    ap.add_argument("--hbase", default="127.0.0.1")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("SpamModelTraining").getOrCreate()
    t_start = time.time()

    # ---- 1. Load from HDFS and clean ----
    raw = spark.read.json(f"{args.hdfs}/data/emails.jsonl")
    df = (with_clean_text(raw).filter(F.length("text") > 0)
          .dropDuplicates(["text"]).select("text", F.col("label").cast("double"))
          .repartition(8).cache())
    n = df.count()
    print(f"\nLoaded {n} unique emails from HDFS ({raw.rdd.getNumPartitions()} input partitions)")

    train, test = df.randomSplit([0.8, 0.2], seed=42)
    train.cache(); test.cache()

    # ---- 2. Train + evaluate each algorithm ----
    results, models = {}, {}
    for key, (name, make) in ALGORITHMS.items():
        t0 = time.time()
        model = Pipeline(stages=feature_stages() + [make()]).fit(train)
        train_secs = time.time() - t0
        m = evaluate(model.transform(test))
        m.update(name=name, train_seconds=round(train_secs, 1))
        results[key], models[key] = m, model

    print("\n" + "=" * 72)
    print(f"{'Algorithm':28}{'Accuracy':>10}{'Precision':>11}{'Recall':>9}{'F1':>8}{'AUC':>8}")
    print("-" * 72)
    for key, m in results.items():
        print(f"{m['name']:28}{m['accuracy']:>10.4f}{m['precision']:>11.4f}{m['recall']:>9.4f}"
              f"{m['f1']:>8.4f}{m['auc']:>8.4f}")
    print("=" * 72)
    c = results[args.deploy]["confusion"]
    print(f"Deployed: {results[args.deploy]['name']}   confusion  TP={c['tp']} TN={c['tn']} "
          f"FP={c['fp']} FN={c['fn']}")

    # ---- 3. Save the deployed model to HDFS ----
    model_path = f"{args.hdfs}/models/spam_model"
    models[args.deploy].write().overwrite().save(model_path)
    print(f"Model saved to {model_path}")

    # ---- 4. Dataset insights (Spark aggregations) ----
    words_df = Pipeline(stages=feature_stages()[:2]).fit(df).transform(df)
    insights = dataset_insights(words_df)

    summary = {
        "deployed": args.deploy,
        "deployed_name": results[args.deploy]["name"],
        "algorithms": results,
        "train_rows": train.count(),
        "test_rows": test.count(),
        "num_features": feature_stages()[2].getNumFeatures(),
        "trained_at": int(time.time() * 1000),
        "total_seconds": round(time.time() - t_start, 1),
        "executors": len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()) - 1,
    }
    save_to_hbase(args.hbase, {"model": summary, "insights": insights})
    spark.stop()


if __name__ == "__main__":
    main()
