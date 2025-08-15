# ======================
# backend/routers/agg.py
# ======================
from __future__ import annotations
from fastapi import APIRouter
from datetime import datetime, timezone
from services.storage import conn
from services.providers import binance_oi_usd_latest, bybit_oi_usd_latest, okx_oi_usd_latest

router = APIRouter(prefix="/agg", tags=["aggregate"])
UTC = timezone.utc
@router.get("/snapshot")
def snapshot_get(symbols: str = "BTCUSDT,ETHUSDT"):
    return snapshot(symbols)

@router.post("/snapshot")
def snapshot(symbols: str = "BTCUSDT,ETHUSDT"):
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    ts = int(datetime.now(UTC).timestamp())
    c = conn()
    out = {}
    for sym in syms:
        values = {}
        b = binance_oi_usd_latest(sym)
        if b is not None: values["binance"] = b
        y = bybit_oi_usd_latest(sym)
        if y is not None: values["bybit"] = y
        o = okx_oi_usd_latest(sym)
        if o is not None: values["okx"] = o
        for ex, v in values.items():
            c.execute("INSERT OR REPLACE INTO oi_snapshots(ts,symbol,exchange,oi_value_usd) VALUES(?,?,?,?)", (ts, sym, ex, float(v)))
        out[sym] = values
    c.commit()
    return {"ts": ts, "data": out}

@router.get("/oi")
def current_oi(symbol: str = "BTCUSDT"):
    b = binance_oi_usd_latest(symbol)
    y = bybit_oi_usd_latest(symbol)
    o = okx_oi_usd_latest(symbol)
    total = sum([v for v in [b, y, o] if v is not None]) if any([b, y, o]) else None
    return {
        "symbol": symbol,
        "exchanges": [
            {"name": "binance", "oi_usd": b},
            {"name": "bybit", "oi_usd": y},
            {"name": "okx", "oi_usd": o},
        ],
        "total_oi_usd": total,
    }

@router.get("/oi_series")
def oi_series(symbol: str = "BTCUSDT", bucket: str = "daily", days: int = 60):
    import pandas as pd
    c = conn()
    df = pd.read_sql_query("SELECT ts, exchange, oi_value_usd FROM oi_snapshots WHERE symbol=? ORDER BY ts ASC", c, params=(symbol,))
    if df.empty:
        return {"series": []}
    df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    df.set_index("ts", inplace=True)
    total = df.groupby(df.index)["oi_value_usd"].sum().sort_index()
    rule = "D" if bucket == "daily" else "H"
    sr = total.resample(rule).last().dropna()
    if days: sr = sr.iloc[-days:]
    out = [{"t": int(t.timestamp()*1000), "oi_usd": float(v)} for t,v in sr.items()]

    return {"symbol": symbol, "bucket": bucket, "series": out}
