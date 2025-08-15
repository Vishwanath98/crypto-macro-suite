# backend/routers/macro.py
# ======================
from __future__ import annotations
from fastapi import APIRouter
from datetime import datetime, timezone
from typing import Optional
import pandas as pd

from services.providers import cg_global, cg_btc_eth_caps
from services.storage import conn

router = APIRouter(prefix="/macro", tags=["macro"])
UTC = timezone.utc
@router.get("/snapshot")
def macro_snapshot_get():
    # Reuse the POST handler
    return macro_snapshot()
    
@router.post("/snapshot")
def macro_snapshot():
    """Store a single snapshot of total/alt/BTC/ETH caps using FREE CoinGecko endpoints (current values).
    Over time (via cron) this builds a historical series without paid APIs.
    """
    g = cg_global() or {}
    caps = cg_btc_eth_caps()
    total = ((g.get("total_market_cap") or {}).get("usd")) or 0.0
    vol = ((g.get("total_volume") or {}).get("usd")) or 0.0
    btc = caps.get("btc") or 0.0
    eth = caps.get("eth") or 0.0
    alt = max(total - btc - eth, 0.0)
    btc_dom = (btc / total * 100.0) if total else None
    ts = int(datetime.now(UTC).timestamp())
    c = conn()
    c.execute("INSERT OR REPLACE INTO macro_snapshots(ts,total_usd,volume_usd,btc_usd,eth_usd,alt_usd,btc_dominance) VALUES(?,?,?,?,?,?,?)",
              (ts, float(total), float(vol), float(btc), float(eth), float(alt), float(btc_dom) if btc_dom is not None else None))
    c.commit()
    return {"ok": True, "ts": ts}

@router.get("/series")
def macro_series(bucket: str = "daily", days: int = 180):
    """Return bucketed macro series from stored snapshots."""
    c = conn()
    df = pd.read_sql_query("SELECT * FROM macro_snapshots ORDER BY ts ASC", c)
    if df.empty:
        return {"series": []}
    df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    df = df.set_index("ts").sort_index()
    rule = "H" if bucket == "hourly" else "D"
    agg = df.resample(rule).last().dropna(how="all")
    if days:
        agg = agg.iloc[-days:]
    out = [
        {
            "t": int(idx.timestamp() * 1000),
            "total": float(row["total_usd"] or 0),
            "volume": float(row["volume_usd"] or 0),
            "btc": float(row["btc_usd"] or 0),
            "eth": float(row["eth_usd"] or 0),
            "alt": float(row["alt_usd"] or 0),
            "btc_dom": float(row["btc_dominance"]) if row["btc_dominance"] is not None else None,
        }
        for idx, row in agg.iterrows()
    ]
    return {"bucket": bucket, "series": out}

