"""Creates the two HBase tables used by the project (safe to run repeatedly).

emails : one row per classified email      families  d = email data, p = prediction
meta   : counters, model metrics, insights  families  c = counters,   m = JSON blobs
"""
import happybase

TABLES = {
    "emails": {"d": dict(max_versions=1), "p": dict(max_versions=1)},
    "meta": {"c": dict(max_versions=1), "m": dict(max_versions=1)},
}

conn = happybase.Connection("127.0.0.1", port=9090, timeout=30000)
existing = {t.decode() for t in conn.tables()}
for name, families in TABLES.items():
    if name in existing:
        print(f"  table '{name}' already exists")
    else:
        conn.create_table(name, families)
        print(f"  created table '{name}' with families {list(families)}")
print("  tables:", [t.decode() for t in conn.tables()])
conn.close()
