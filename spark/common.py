"""Text cleaning + feature stages shared by training and streaming.

Using the SAME code in both jobs guarantees an email is processed identically
when the model is trained and when it is used in real time.
All cleaning uses built-in Spark SQL functions (runs in the JVM on the workers;
no slow Python UDFs).
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StopWordsRemover

NUM_FEATURES = 1 << 16  # 65,536 hashed word buckets: small memory footprint


def with_clean_text(df: DataFrame) -> DataFrame:
    """Adds a normalised `text` column from `subject` + `body`."""
    t = F.lower(F.concat_ws(" ", F.coalesce(F.col("subject"), F.lit("")),
                            F.coalesce(F.col("body"), F.lit(""))))
    t = F.regexp_replace(t, r"<[^>]+>", " ")                          # HTML tags
    # Links and addresses; also matches the spaced-out form used in the Enron
    # corpus ("http : / / www . site . com", "name @ site . com")
    t = F.regexp_replace(t, r"(https?\s*:\s*/\s*/\s*|www\s*\.\s*)[a-z0-9-]+(\s*[./?=&]\s*[a-z0-9-]+)*",
                         " urltoken ")
    t = F.regexp_replace(t, r"[a-z0-9._-]+\s*@\s*[a-z0-9-]+(\s*\.\s*[a-z0-9-]+)+", " emailtoken ")
    t = F.regexp_replace(t, r"[$£€]\s?\d[\d,.]*", " moneytoken ")     # money amounts
    t = F.regexp_replace(t, r"\d+", " numtoken ")                     # other numbers
    t = F.regexp_replace(t, r"\s+", " ")
    return df.withColumn("text", F.trim(t))


def feature_stages():
    """text -> words -> without stop-words -> term frequencies -> TF-IDF vector."""
    return [
        RegexTokenizer(inputCol="text", outputCol="tokens", pattern="[^a-z]+", minTokenLength=2),
        StopWordsRemover(inputCol="tokens", outputCol="words"),
        HashingTF(inputCol="words", outputCol="tf", numFeatures=NUM_FEATURES),
        IDF(inputCol="tf", outputCol="features", minDocFreq=2),
    ]
