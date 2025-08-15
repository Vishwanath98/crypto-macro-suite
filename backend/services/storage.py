# backend/services/storage.py
# ======================
from __future__ import annotations
import os, sqlite3

DB_PATH = os.environ.get("DB_PATH", "data.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS macro_snapshots (
  ts INTEGER PRIMARY KEY,            -- seconds epoch (UTC)
  total_usd REAL,
  volume_usd REAL,
  btc_usd REAL,
  eth_usd REAL,
  alt_usd REAL,
  btc_dominance REAL
);

CREATE TABLE IF NOT EXISTS oi_snapshots (
  ts INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  exchange TEXT NOT NULL,
  oi_value_usd REAL,
  PRIMARY KEY (ts, symbol, exchange)
);

CREATE INDEX IF NOT EXISTS idx_macro_ts ON macro_snapshots(ts);
CREATE INDEX IF NOT EXISTS idx_oi_ts ON oi_snapshots(ts);
"""

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

_CONN = None

def init_db():
    global _CONN
    _CONN = get_conn()
    _CONN.executescript(SCHEMA)
    _CONN.commit()


def conn():
    global _CONN
    if _CONN is None:
        init_db()
    return _CONN