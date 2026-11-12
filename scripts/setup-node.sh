#!/usr/bin/env bash
# One-time installation on a Mac (no Homebrew, no sudo needed).
#   Master:  scripts/setup-node.sh master
#   Worker:  scripts/setup-node.sh worker
# Installs into /Users/Shared/bigdata (same path on every Mac).
set -euo pipefail
ROLE="${1:-}"
if [ "$ROLE" != "master" ] && [ "$ROLE" != "worker" ]; then
  echo "usage: $0 master|worker"; exit 1
fi
. "$(dirname "$0")/common.sh"

mkdir -p "$BIG"/{downloads,data,logs,run,conf,bin}

download() {  # download URL FILE  (skips if already present, e.g. copied by AirDrop/USB)
  local out="$BIG/downloads/$2"
  if [ -s "$out" ]; then ok "already downloaded: $2"; return; fi
  log "downloading $2"
  curl -fL --retry 3 -o "$out.part" "$1" && mv "$out.part" "$out"
}

extract() {  # extract FILE TARGET_DIR_NAME  (tarball has one top-level folder)
  local target="$BIG/$2"
  if [ -d "$target" ]; then ok "already installed: $2"; return; fi
  log "extracting $1"
  rm -rf "$BIG/.extract"; mkdir -p "$BIG/.extract"
  tar -xzf "$BIG/downloads/$1" -C "$BIG/.extract"
  mv "$BIG/.extract"/* "$target"
  rm -rf "$BIG/.extract"
}

# ---- Java 11 (Temurin, Apple Silicon) ----
download "$JDK_URL" jdk11.tar.gz
extract jdk11.tar.gz jdk-dist
ln -sfn "$BIG/jdk-dist/Contents/Home" "$BIG/jdk"

# ---- Hadoop + Spark (every node) ----
download "$HADOOP_URL" "hadoop-$HADOOP_VER.tar.gz"
extract "hadoop-$HADOOP_VER.tar.gz" hadoop
download "$SPARK_URL" "spark-$SPARK_VER-bin-hadoop3.tgz"
extract "spark-$SPARK_VER-bin-hadoop3.tgz" spark

# ---- Kafka + HBase (master only) ----
if [ "$ROLE" = "master" ]; then
  download "$KAFKA_URL" "kafka_2.13-$KAFKA_VER.tgz"
  extract "kafka_2.13-$KAFKA_VER.tgz" kafka
  download "$HBASE_URL" "hbase-$HBASE_VER-hadoop3-bin.tar.gz"
  extract "hbase-$HBASE_VER-hadoop3-bin.tar.gz" hbase
fi

# Files copied via AirDrop get a quarantine flag that blocks execution
xattr -dr com.apple.quarantine "$BIG" 2>/dev/null || true

# ---- Python 3.11 virtualenv via uv (identical path on every node) ----
if [ ! -x "$BIG/bin/uv" ]; then
  log "installing uv (Python manager)"
  curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$BIG/bin" UV_NO_MODIFY_PATH=1 INSTALLER_NO_MODIFY_PATH=1 sh
fi
export UV_PYTHON_INSTALL_DIR="$BIG/python"
if [ ! -x "$BIG/venv/bin/python" ]; then
  log "creating Python $PY_VER virtualenv"
  "$BIG/bin/uv" venv --python "$PY_VER" "$BIG/venv"
fi
log "installing Python packages"
"$BIG/bin/uv" pip install --python "$BIG/venv/bin/python" -r "$PROJECT_DIR/requirements-$ROLE.txt"

# ---- Render config files for this machine ----
"$PROJECT_DIR/scripts/configure.sh" "$ROLE"

if [ "$ROLE" = "master" ]; then
  # ---- Dataset ----
  if [ ! -s "$PROJECT_DIR/data/emails.jsonl" ]; then
    mkdir -p "$PROJECT_DIR/data"
    [ -s "$PROJECT_DIR/data/enron_spam_data.zip" ] || {
      log "downloading Enron-Spam dataset"
      curl -fL --retry 3 -o "$PROJECT_DIR/data/enron_spam_data.zip" "$DATASET_URL"
    }
    "$BIG/venv/bin/python" "$PROJECT_DIR/data/prepare_data.py"
  fi
  # ---- Frontend build (served by FastAPI) ----
  if [ ! -f "$PROJECT_DIR/frontend/package.json" ]; then
    warn "frontend/ not found - skipping UI build"
  elif command -v npm >/dev/null 2>&1; then
    log "building frontend"
    (cd "$PROJECT_DIR/frontend" && npm install --no-fund --no-audit && npm run build)
  else
    warn "npm not found - install Node.js from nodejs.org, then run: cd frontend && npm install && npm run build"
  fi
fi

ok "setup complete for $ROLE ($THIS_IP).  Master IP = $MASTER_IP"
