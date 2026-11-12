"""FastAPI backend for the spam dashboard.

  POST /api/classify  -> publishes the email to Kafka, waits for the Spark
                         streaming job to write the prediction into HBase
  GET  /api/emails    -> classification history (HBase scan, newest first)
  GET  /api/stats     -> live counters + per-minute timeline (HBase counters)
  GET  /api/model     -> training metrics + dataset insights (HBase `meta`)
  GET  /api/cluster   -> live status of HDFS, Spark, Kafka, HBase
Also serves the built React app (frontend/dist) at "/".

Run with scripts/run-app.sh
"""
import imaplib
import json
import os
import struct
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import happybase
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from pydantic import BaseModel, Field

from producer import gmail_fetcher

MASTER_IP = os.environ.get("MASTER_IP", "127.0.0.1")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", f"{MASTER_IP}:9092")
TOPIC = "emails"
HBASE_HOST = os.environ.get("HBASE_THRIFT_HOST", "127.0.0.1")
MAX_TS = 9_999_999_999_999  # same rowkey scheme as producer/producer.py

app = FastAPI(title="Email Spam Classification API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

pool = happybase.ConnectionPool(size=4, host=HBASE_HOST, port=9090, timeout=10000)
_producer = None


def hb(fn):
    """Runs fn(connection) on a pooled HBase connection. The Thrift server closes
    idle sockets; happybase then refreshes the connection, so one retry is enough."""
    for attempt in range(2):
        try:
            with pool.connection() as conn:
                return fn(conn)
        except Exception:
            if attempt:
                raise


def producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(bootstrap_servers=KAFKA, linger_ms=0,
                                  value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                                  request_timeout_ms=5000, max_block_ms=5000)
    return _producer


def decode_row(key: bytes, data: dict) -> dict:
    d = {k.decode().split(":", 1)[1]: v.decode("utf-8", "replace") for k, v in data.items()}
    out = {"rowkey": key.decode(), **d}
    for f in ("p_spam", "confidence"):
        if f in out:
            out[f] = float(out[f])
    for f in ("latency_ms", "sent_at", "processed_at", "batch_id", "kafka_partition", "kafka_offset"):
        if out.get(f, "") != "":
            out[f] = int(out[f])
    out["true_label"] = None if out.get("true_label", "") == "" else int(out["true_label"])
    return out


def counter(value: bytes | None) -> int:
    return struct.unpack(">q", value)[0] if value else 0


# ------------------------------------------------------------------ classify
class EmailIn(BaseModel):
    subject: str = Field("", max_length=500)
    body: str = Field(..., min_length=1, max_length=20000)
    sender: str = Field("user@dashboard.local", max_length=200)


@app.post("/api/classify")
def classify(email: EmailIn):
    sent_at = int(time.time() * 1000)
    eid = uuid.uuid4().hex[:12]
    rowkey = f"{MAX_TS - sent_at:013d}-{eid}"
    try:
        producer().send(TOPIC, {"rowkey": rowkey, "id": eid, "subject": email.subject,
                                "body": email.body, "sender": email.sender, "source": "dashboard",
                                "true_label": None, "sent_at": sent_at}).get(timeout=5)
    except Exception as e:
        raise HTTPException(503, f"Kafka unavailable at {KAFKA}: {e}")

    # Wait for Spark Streaming to write the result into HBase
    deadline = time.time() + 20
    while time.time() < deadline:
        row = hb(lambda conn: conn.table("emails").row(rowkey.encode()))
        if row:
            result = decode_row(rowkey.encode(), row)
            result["roundtrip_ms"] = int(time.time() * 1000) - sent_at
            return result
        time.sleep(0.25)
    raise HTTPException(504, "Email was queued in Kafka but no prediction arrived within 20 s. "
                             "Is the streaming job running? (scripts/run-stream.sh)")


# ------------------------------------------------------------------ history
@app.get("/api/emails")
def emails(limit: int = Query(50, le=500), label: str | None = None, source: str | None = None):
    filtered = label or source
    scanned = hb(lambda conn: list(conn.table("emails").scan(limit=limit * 20 if filtered else limit)))
    out = []
    for key, data in scanned:
        row = decode_row(key, data)
        row["body"] = row.get("body", "")[:400]
        if (label and row.get("label") != label) or (source and row.get("source") != source):
            continue
        out.append(row)
        if len(out) >= limit:
            break
    return out


@app.get("/api/emails/{rowkey}")
def email(rowkey: str):
    row = hb(lambda conn: conn.table("emails").row(rowkey.encode()))
    if not row:
        raise HTTPException(404, "not found")
    return decode_row(rowkey.encode(), row)


# ------------------------------------------------------------------ stats
@app.get("/api/stats")
def stats(minutes: int = Query(30, le=240)):
    now = datetime.now()
    start = (now - timedelta(minutes=minutes - 1)).strftime("%Y%m%d%H%M")
    def read(conn):
        meta = conn.table("meta")
        return meta.row(b"stats"), {k.decode()[4:]: v for k, v in
                                    meta.scan(row_start=f"min_{start}".encode(), row_stop=b"min_~")}
    s, buckets = hb(read)
    timeline = []
    for i in range(minutes):
        t = now - timedelta(minutes=minutes - 1 - i)
        b = buckets.get(t.strftime("%Y%m%d%H%M"), {})
        timeline.append({"time": t.strftime("%H:%M"), "spam": counter(b.get(b"c:spam")),
                         "ham": counter(b.get(b"c:ham"))})
    total, spam = counter(s.get(b"c:total")), counter(s.get(b"c:spam"))
    labelled, correct = counter(s.get(b"c:labelled")), counter(s.get(b"c:correct"))
    return {"total": total, "spam": spam, "ham": counter(s.get(b"c:ham")),
            "spam_rate": spam / total if total else 0.0,
            "labelled": labelled, "live_accuracy": correct / labelled if labelled else None,
            "timeline": timeline}


# ------------------------------------------------------------------ model
@app.get("/api/model")
def model():
    rows = dict(hb(lambda conn: conn.table("meta").rows([b"model", b"insights"])))
    if b"model" not in rows:
        raise HTTPException(404, "No model metrics yet - run scripts/run-train.sh")
    return {"model": json.loads(rows[b"model"][b"m:json"]),
            "insights": json.loads(rows[b"insights"][b"m:json"]) if b"insights" in rows else None}


# ------------------------------------------------------------------ cluster
def _get(url):
    return requests.get(url, timeout=2).json()


@app.get("/api/cluster")
def cluster():
    out = {"master_ip": MASTER_IP}

    try:  # HDFS (NameNode JMX)
        nn = _get(f"http://{MASTER_IP}:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo")["beans"][0]
        fs = _get(f"http://{MASTER_IP}:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState")["beans"][0]
        live = json.loads(nn["LiveNodes"])
        out["hdfs"] = {"up": True, "live_datanodes": fs["NumLiveDataNodes"],
                       "dead_datanodes": fs["NumDeadDataNodes"], "files": fs.get("FilesTotal"),
                       "blocks": fs.get("BlocksTotal"), "capacity": nn["Total"], "used": nn["Used"],
                       "datanodes": [{"name": k.split(":")[0], "used": v.get("usedSpace", 0),
                                      "capacity": v.get("capacity", 0), "blocks": v.get("numBlocks", 0)}
                                     for k, v in live.items()],
                       "ui": f"http://{MASTER_IP}:9870"}
    except Exception as e:
        out["hdfs"] = {"up": False, "error": str(e)}

    try:  # Spark standalone master
        sp = _get(f"http://{MASTER_IP}:8080/json/")
        out["spark"] = {"up": True, "alive_workers": sp.get("aliveworkers", 0),
                        "cores": sp.get("cores"), "cores_used": sp.get("coresused"),
                        "workers": [{"host": w["host"], "cores": w["cores"], "cores_used": w["coresused"],
                                     "memory": w["memory"], "memory_used": w["memoryused"], "state": w["state"]}
                                    for w in sp.get("workers", [])],
                        "apps": [{"name": a["name"], "cores": a["cores"], "state": a["state"]}
                                 for a in sp.get("activeapps", [])],
                        "ui": f"http://{MASTER_IP}:8080"}
    except Exception as e:
        out["spark"] = {"up": False, "error": str(e)}

    try:  # Kafka
        c = KafkaConsumer(bootstrap_servers=KAFKA, request_timeout_ms=3000)
        parts = sorted(c.partitions_for_topic(TOPIC) or [])
        ends = c.end_offsets([TopicPartition(TOPIC, p) for p in parts])
        c.close()
        out["kafka"] = {"up": True, "broker": KAFKA, "topic": TOPIC,
                        "partitions": [{"partition": tp.partition, "end_offset": off}
                                       for tp, off in sorted(ends.items(), key=lambda x: x[0].partition)],
                        "messages": sum(ends.values())}
    except Exception as e:
        out["kafka"] = {"up": False, "error": str(e)}

    try:  # HBase via Thrift
        tables = [t.decode() for t in hb(lambda conn: conn.tables())]
        out["hbase"] = {"up": True, "tables": tables, "thrift": f"{HBASE_HOST}:9090",
                        "ui": f"http://{MASTER_IP}:16010"}
    except Exception as e:
        out["hbase"] = {"up": False, "error": str(e)}
    return out


# ------------------------------------------------------------------ gmail
@app.get("/api/gmail/status")
def gmail_status():
    user = gmail_fetcher.masked_user()
    return {"configured": user is not None, "user": user}


@app.post("/api/gmail/fetch")
def gmail_fetch(limit: int = Query(10, ge=1, le=50)):
    """Reads the newest unseen Gmail messages (read-only) and sends them through Kafka."""
    try:
        return gmail_fetcher.fetch_and_publish(KAFKA, limit)
    except gmail_fetcher.GmailNotConfigured as e:
        raise HTTPException(400, str(e))
    except imaplib.IMAP4.error as e:
        raise HTTPException(401, f"Gmail login failed: {e}. Check the App Password in gmail.env.")
    except Exception as e:
        raise HTTPException(502, f"Could not fetch from Gmail: {e}")


@app.get("/api/health")
def health():
    return {"ok": True}


# ------------------------------------------------------------------ frontend
DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
if DIST.exists():
    app.mount("/", StaticFiles(directory=DIST, html=True), name="ui")
