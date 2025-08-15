from fastapi import APIRouter, Query
from services.db import _conn
from datetime import datetime, timezone
import requests, time

router = APIRouter()
UA = {"User-Agent":"Mozilla/5.0","Accept":"application/json"}

def _now_ms(): return int(time.time()*1000)

def _coingecko_global():
    # free public snapshot of total/volume/btc_dom
    r = requests.get("https://api.coingecko.com/api/v3/global", headers=UA, timeout=20)
    r.raise_for_status()
    g = r.json().get("data", {})
    total = g.get("total_market_cap", {}).get("usd")
    volume = g.get("total_volume", {}).get("usd")
    btc_dom = g.get("market_cap_percentage", {}).get("btc")
    # rough split into btc/eth/alt (best-effort, still free)
    btc = g.get("market_cap_percentage", {}).get("btc", 0)/100.0 * total if total else None
    eth = g.get("market_cap_percentage", {}).get("eth", 0)/100.0 * total if total else None
    alt = total - (btc or 0) - (eth or 0) if total else None
    return {"total": total, "volume": volume, "btc": btc, "eth": eth, "alt": alt, "btc_dom": btc_dom}

@router.get("/snapshot")
@router.post("/snapshot")
def snapshot():
    try:
        t = _now_ms()
        g = _coingecko_global()
        con = _conn()
        con.execute("""INSERT OR REPLACE INTO macro_snapshots
                       (time_ms,total,volume,btc,eth,alt,btc_dom,fear_greed)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (t, g["total"], g["volume"], g["btc"], g["eth"], g["alt"], g["btc_dom"], None))
        con.commit(); con.close()
        return {"ok": True, "t": t, **g}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@router.get("/series")
def series(bucket: str = "daily", days: int = 180):
    import pandas as pd
    now = int(datetime.now(timezone.utc).timestamp()*1000)
    since = now - days*24*3600*1000
    con = _conn()
    df = pd.read_sql_query("SELECT * FROM macro_snapshots WHERE time_ms>=?", con, params=(since,))
    con.close()
    if df.empty:
        return {"series": []}
    # daily bucket (simple)
    df["t"] = (df["time_ms"]//(24*3600*1000))*(24*3600*1000)
    cols = ["total","volume","btc","eth","alt","btc_dom","fear_greed"]
    agg = df.groupby("t", as_index=False)[cols].mean(numeric_only=True)
    return {"series": agg.to_dict(orient="records")}
