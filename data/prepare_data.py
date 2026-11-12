"""Converts the Enron-Spam CSV (inside enron_spam_data.zip) into JSON Lines.

Why JSON Lines?  One email per line -> HDFS can split the file into blocks and
Spark can read the blocks in parallel on different workers (a multi-line CSV cannot).

Output: data/emails.jsonl   {"id", "subject", "body", "label"}   label: 1 = spam, 0 = ham
"""
import csv
import io
import json
import sys
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
src = HERE / "enron_spam_data.zip"
out = HERE / "emails.jsonl"

csv.field_size_limit(sys.maxsize)
with zipfile.ZipFile(src) as z:
    name = next(n for n in z.namelist() if n.endswith(".csv"))
    rows = csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8", errors="replace"))
    n = spam = 0
    with out.open("w", encoding="utf-8") as f:
        for r in rows:
            label = 1 if r["Spam/Ham"].strip().lower() == "spam" else 0
            f.write(json.dumps({
                "id": r.get("Message ID", str(n)),
                "subject": (r.get("Subject") or "").strip(),
                "body": (r.get("Message") or "").strip(),
                "label": label,
            }) + "\n")
            n += 1
            spam += label
print(f"wrote {n} emails ({spam} spam, {n - spam} ham) -> {out}")
