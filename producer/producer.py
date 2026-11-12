"""Streams labelled emails from the dataset into the Kafka topic `emails`.

Simulates a mail server receiving emails. Each message keeps its true label so
the dashboard can show live accuracy.

  python producer/producer.py --rate 20 --count 500
"""
import argparse
import json
import random
import time
import uuid
from pathlib import Path

from kafka import KafkaProducer

MAX_TS = 9_999_999_999_999  # rowkey = reversed timestamp -> newest rows first in HBase


def make_rowkey(sent_at_ms: int, email_id: str) -> str:
    return f"{MAX_TS - sent_at_ms:013d}-{email_id}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka", default="localhost:9092")
    ap.add_argument("--topic", default="emails")
    ap.add_argument("--rate", type=float, default=20, help="emails per second")
    ap.add_argument("--count", type=int, default=500, help="how many emails to send")
    ap.add_argument("--file", default=str(Path(__file__).resolve().parent.parent / "data/emails.jsonl"))
    args = ap.parse_args()

    with open(args.file, encoding="utf-8") as f:
        emails = [json.loads(line) for line in f]
    random.shuffle(emails)

    producer = KafkaProducer(bootstrap_servers=args.kafka,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                             linger_ms=20)
    print(f"Sending {args.count} emails to Kafka topic '{args.topic}' at {args.rate}/s ...")
    delay = 1.0 / args.rate if args.rate > 0 else 0
    start = time.time()
    for i, e in enumerate(emails[:args.count], 1):
        sent_at = int(time.time() * 1000)
        eid = uuid.uuid4().hex[:12]
        sender = f"user{random.randint(1, 999)}@{'promo-mail.biz' if e['label'] else 'enron.com'}"
        producer.send(args.topic, {
            "rowkey": make_rowkey(sent_at, eid), "id": eid,
            "subject": e["subject"], "body": e["body"][:5000], "sender": sender,
            "source": "producer", "true_label": e["label"], "sent_at": sent_at,
        })
        if i % 100 == 0:
            print(f"  sent {i}")
        if delay:
            time.sleep(max(0.0, start + i * delay - time.time()))
    producer.flush()
    print(f"Done: {args.count} emails in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
