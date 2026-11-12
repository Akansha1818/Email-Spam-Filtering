"""Reads emails from a real Gmail inbox and sends them into Kafka for classification.

  * Uses IMAP over SSL in READ-ONLY mode: nothing is marked as read, moved or deleted.
  * Credentials come from gmail.env in the project root (git-ignored):
        GMAIL_USER=you@gmail.com
        GMAIL_APP_PASSWORD=abcd efgh ijkl mnop
  * Remembers which messages were already sent (data/gmail_seen.json), so each
    email is classified only once.

  python producer/gmail_fetcher.py --limit 10           # latest 10 new emails
  python producer/gmail_fetcher.py --watch 30           # keep checking every 30 s
"""
import argparse
import email
import html
import imaplib
import json
import os
import re
import time
import uuid
from email.header import decode_header, make_header
from email.utils import parseaddr
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "gmail.env"
STATE = ROOT / "data" / "gmail_seen.json"
MAX_TS = 9_999_999_999_999  # same rowkey scheme as producer.py / backend


class GmailNotConfigured(Exception):
    pass


def load_config():
    cfg = {}
    if CONFIG.exists():
        for line in CONFIG.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip().strip('"').strip("'")
    user = os.environ.get("GMAIL_USER") or cfg.get("GMAIL_USER", "")
    password = (os.environ.get("GMAIL_APP_PASSWORD") or cfg.get("GMAIL_APP_PASSWORD", "")).replace(" ", "")
    if not user or not password or "you@" in user:
        raise GmailNotConfigured(f"Gmail is not configured. Copy gmail.env.example to {CONFIG.name} and fill it in.")
    return user, password


def masked_user():
    try:
        user, _ = load_config()
    except GmailNotConfigured:
        return None
    name, _, domain = user.partition("@")
    return f"{name[:2]}{'*' * max(1, len(name) - 2)}@{domain}"


def _decode(value) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return str(value)


def _html_to_text(s: str) -> str:
    s = re.sub(r"(?is)<(script|style).*?</\1>", " ", s)
    s = re.sub(r"(?i)<br\s*/?>|</p>|</div>|</tr>", "\n", s)
    # keep link targets so URLs still count as a spam signal
    s = re.sub(r'(?i)<a\s[^>]*href="([^"]+)"[^>]*>', r" \1 ", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    return re.sub(r"[ \t\r\f\v]+", " ", re.sub(r"\n\s*\n+", "\n\n", s)).strip()


def _body(msg) -> str:
    plain, rich = [], []
    for part in msg.walk() if msg.is_multipart() else [msg]:
        if part.get_content_maintype() == "multipart" or part.get_filename():
            continue
        ctype = part.get_content_type()
        if ctype not in ("text/plain", "text/html"):
            continue
        payload = part.get_payload(decode=True) or b""
        text = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
        (plain if ctype == "text/plain" else rich).append(text)
    if plain and any(p.strip() for p in plain):
        return "\n".join(plain).strip()
    return _html_to_text("\n".join(rich))


def parse_message(raw: bytes) -> dict:
    msg = email.message_from_bytes(raw)
    name, addr = parseaddr(_decode(msg.get("From")))
    return {
        "subject": _decode(msg.get("Subject")).strip(),
        "sender": addr or name or "unknown",
        "body": _body(msg)[:20000],
        "date": msg.get("Date", ""),
    }


def _load_seen() -> set:
    try:
        return set(json.loads(STATE.read_text()))
    except Exception:
        return set()


def _save_seen(seen: set):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(sorted(seen)))


def fetch(limit: int = 10, include_seen: bool = False, mailbox: str = "INBOX"):
    """Returns up to `limit` of the newest messages as (key, parsed_email) pairs."""
    user, password = load_config()
    seen = set() if include_seen else _load_seen()
    out = []
    with imaplib.IMAP4_SSL("imap.gmail.com", 993, timeout=30) as m:
        m.login(user, password)
        typ, info = m.select(mailbox, readonly=True)          # read-only: never changes the inbox
        if typ != "OK":
            raise RuntimeError(f"cannot open mailbox {mailbox}")
        validity = (m.response("UIDVALIDITY")[1] or [b"0"])[0]
        validity = validity.decode() if isinstance(validity, bytes) else str(validity)
        typ, data = m.uid("search", None, "ALL")
        uids = data[0].split() if data and data[0] else []
        for uid in reversed(uids):                             # newest first
            key = f"{validity}:{uid.decode()}"
            if key in seen:
                continue
            typ, parts = m.uid("fetch", uid, "(BODY.PEEK[])")  # PEEK: does not mark as read
            raw = next((p[1] for p in parts if isinstance(p, tuple)), None)
            if raw:
                out.append((key, parse_message(raw)))
            if len(out) >= limit:
                break
    return out


def fetch_and_publish(kafka: str, limit: int = 10, include_seen: bool = False, topic: str = "emails") -> dict:
    """Fetches new Gmail messages and publishes them to Kafka. Returns a summary."""
    from kafka import KafkaProducer

    messages = fetch(limit, include_seen)
    if not messages:
        return {"published": 0, "emails": []}
    producer = KafkaProducer(bootstrap_servers=kafka, linger_ms=5,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    seen = _load_seen()
    summary = []
    for key, e in messages:
        sent_at = int(time.time() * 1000)
        eid = uuid.uuid4().hex[:12]
        rowkey = f"{MAX_TS - sent_at:013d}-{eid}"
        producer.send(topic, {"rowkey": rowkey, "id": eid, "subject": e["subject"], "body": e["body"],
                              "sender": e["sender"], "source": "gmail", "true_label": None,
                              "sent_at": sent_at})
        seen.add(key)
        summary.append({"rowkey": rowkey, "subject": e["subject"], "sender": e["sender"]})
        time.sleep(0.002)  # distinct timestamps -> distinct, ordered row keys
    producer.flush()
    _save_seen(seen)
    return {"published": len(summary), "emails": summary}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--kafka", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--limit", type=int, default=10, help="max emails per check")
    ap.add_argument("--all", action="store_true", help="include emails already classified before")
    ap.add_argument("--watch", type=int, default=0, metavar="SECONDS",
                    help="keep running and check for new emails every SECONDS")
    args = ap.parse_args()

    while True:
        try:
            r = fetch_and_publish(args.kafka, args.limit, args.all)
            stamp = time.strftime("%H:%M:%S")
            if r["published"]:
                print(f"[{stamp}] sent {r['published']} Gmail email(s) to Kafka:")
                for e in r["emails"]:
                    print(f"    - {e['subject'][:70] or '(no subject)'}   <{e['sender']}>")
            else:
                print(f"[{stamp}] no new emails")
        except GmailNotConfigured as e:
            raise SystemExit(str(e))
        except imaplib.IMAP4.error as e:
            raise SystemExit(f"Gmail login failed ({e}). Check GMAIL_USER and the 16-letter App Password.")
        if not args.watch:
            break
        args.all = False
        time.sleep(args.watch)


if __name__ == "__main__":
    main()
