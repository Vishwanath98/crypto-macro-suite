# FastAPI Backend — OI Aggregator + Persistence (SQLite)
from __future__ import annotations
import os, sqlite3, time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

UTC = timezone.utc
DB_PATH = os.environ.get("DB_PATH", "data.db")
SYMBOLS = [s.strip().upper() for s in os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
ENABLE_BYBIT = os.environ.get("ENABLE_BYBIT", "0") == "1"
ENABLE_OKX = os.environ.get("ENABLE_OKX", "0") == "1"

app = FastAPI(title="Crypto OI Aggregator")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

BINANCE_FAPI = "https://fapi.binance.com"

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

CONN = db()
CONN.execute("""
    CREATE TABLE IF NOT EXISTS oi_snapshots (
        ts INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        exchange TEXT NOT NULL,
        oi_value_usd REAL,
        PRIMARY KEY (ts, symbol, exchange)
    )
""")
CONN.commit()

def binance_oi_usd(symbol: str) -> Optional[float]:
    url = f"{BINANCE_FAPI}/futures/data/openInterestHist"
    params = {"symbol": symbol, "period": "5m", "limit": 1}
    try:
        js = requests.get(url, params=params, timeout=15).json()
        if isinstance(js, list) and js:
            v = js[0].get("sumOpenInterestValue")
            return float(v) if v is not None else None
    except Exception:
        return None
    return None

def bybit_oi_usd(symbol: str) -> Optional[float]:
    if not ENABLE_BYBIT:
        return None
    try:
        url = "https://api.bybit.com/v5/market/open-interest"
        params = {"category": "linear", "symbol": symbol, "interval": "5min"}
        js = requests.get(url, params=params, timeout=15).json()
        data = (((js or {}).get("result") or {}).get("list") or [])
        if data:
            v = data[-1].get("openInterest")
            return float(v) if v is not None else None
    except Exception:
        return None
    return None

def okx_oi_usd(symbol: str) -> Optional[float]:
    if not ENABLE_OKX:
        return None
    try:
        inst = symbol.replace("USDT", "-USDT-SWAP")
        url = "https://www.okx.com/api/v5/public/open-interest"
        js = requests.get(url, params={"instId": inst}, timeout=15).json()
        data = ((js or {}).get("data") or [])
        if data:
            oi_ccy = data[0].get("oiCcy")
            return float(oi_ccy) if oi_ccy is not None else None
    except Exception:
        return None
    return None

EX_PROVIDERS = {
    "binance": binance_oi_usd,
    "bybit": bybit_oi_usd,
    "okx": okx_oi_usd,
}

def snapshot_once(symbols: List[str]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    ts = int(datetime.now(UTC).timestamp())
    cur = CONN.cursor()
    for sym in symbols:
        sym_res: Dict[str, float] = {}
        for ex, fn in EX_PROVIDERS.items():
            v = fn(sym)
            if v is None:
                continue
            sym_res[ex] = v
            cur.execute(
                "INSERT OR REPLACE INTO oi_snapshots (ts, symbol, exchange, oi_value_usd) VALUES (?,?,?,?)",
                (ts, sym, ex, float(v)),
            )
        out[sym] = sym_res
    CONN.commit()
    return out

@app.get("/health")
def health():
    return {"ok": True, "symbols": SYMBOLS}

@app.post("/snapshot")
def snapshot_now():
    data = snapshot_once(SYMBOLS)
    return {"ok": True, "data": data}

@app.get("/oi/{symbol}")
def get_oi(symbol: str):
    symbol = symbol.upper()
    data = snapshot_once([symbol])[symbol]
    rows = []
    for ex, val in data.items():
        rows.append({"name": ex, "oi_value_usd": val, "timestamp": datetime.now(UTC).isoformat()})
    return {"symbol": symbol, "exchanges": rows, "total_oi_usd": float(sum(data.values())) if data else None, "as_of": datetime.now(UTC).isoformat()}

@app.get("/stats/oi/series")
def oi_series(symbol: str, bucket: str = "daily", days: int = 30):
    symbol = symbol.upper()
    df = pd.read_sql_query(
        "SELECT ts, exchange, oi_value_usd FROM oi_snapshots WHERE symbol=? ORDER BY ts ASC",
        CONN, params=(symbol,), dtype={"exchange": str, "oi_value_usd": float}
    )
    if df.empty:
        return {"symbol": symbol, "series": []}
    df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    df.set_index("ts", inplace=True)
    total = df.groupby(df.index)["oi_value_usd"].sum().sort_index()
    rule = "D" if bucket == "daily" else "W"
    sr = total.resample(rule).last().dropna()
    if days:
        sr = sr.iloc[-days:]
    series = [{"ts": t.isoformat(), "oi_value_usd": float(v)} for t, v in sr.items()]
    return {"symbol": symbol, "bucket": bucket, "series": series}

@app.get("/stats/oi/summary")
def oi_summary(symbol: str, bucket: str = "daily", days: int = 30):
    res = oi_series(symbol, bucket, days)
    ser = res.get("series", [])
    if len(ser) < 2:
        return {"symbol": symbol, "bucket": bucket, "days": days, "change_pct": None}
    first, last = ser[0]["oi_value_usd"], ser[-1]["oi_value_usd"]
    change_pct = (last - first) / first * 100 if first else None
    return {"symbol": symbol, "bucket": bucket, "days": days, "start": first, "end": last, "change_pct": change_pct}
