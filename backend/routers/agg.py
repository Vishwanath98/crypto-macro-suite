from fastapi import APIRouter, Query
import requests, time
from typing import List, Dict, Any
from services.db import _conn

router = APIRouter()

UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

def _jget(url, params=None, base_headers=None, timeout=15):
    try:
        r = requests.get(url, headers=base_headers or UA, params=params or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_error": str(e), "_url": url}

def _now_ms(): return int(time.time()*1000)

def _binance_oi(symbol: str) -> Dict[str, Any]:
    # Present OI notional (sumOpenInterestValue) (public)
    js = _jget("https://fapi.binance.com/futures/data/openInterestHist",
               {"symbol": symbol, "period": "5m", "limit": 1})
    if isinstance(js, list) and js:
        row = js[-1]
        return {"exchange":"binance", "oi_usd": float(row.get("sumOpenInterestValue", 0))}
    # fallback: present OI (contracts) * mark price (approx)
    pres = _jget("https://fapi.binance.com/fapi/v1/openInterest", {"symbol": symbol})
    if pres.get("openInterest"):
        oi_ct = float(pres["openInterest"])
        mark = _jget("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol": symbol}).get("markPrice")
        if mark: return {"exchange":"binance", "oi_usd": oi_ct * float(mark)}
    return {"exchange":"binance", "oi_usd": None}

def _bybit_oi(symbol: str) -> Dict[str, Any]:
    # v5 market open-interest (public)
    js = _jget("https://api.bybit.com/v5/market/open-interest",
               {"category":"linear","symbol":symbol,"interval":"5min","limit":1})
    try:
        lst = js.get("result", {}).get("list", [])
        if lst:
            last = lst[-1]
            val = float(last.get("openInterestValue") or last.get("openInterestUsd") or 0)
            return {"exchange":"bybit", "oi_usd": val or None}
    except Exception:
        pass
    return {"exchange":"bybit", "oi_usd": None}

def _okx_inst(symbol: str) -> str:
    # map "BTCUSDT" -> "BTC-USDT-SWAP"
    base = symbol[:-4]; quote = symbol[-4:]
    return f"{base}-{quote}-SWAP"

def _okx_oi(symbol: str) -> Dict[str, Any]:
    inst = _okx_inst(symbol)
    js = _jget("https://www.okx.com/api/v5/public/open-interest", {"instId": inst})
    try:
        data = js.get("data", [])
        if data:
            # OKX returns "oi" in contracts; get last price for notional
            oi_ct = float(data[0].get("oi", 0))
            tk = _jget("https://www.okx.com/api/v5/market/ticker", {"instId": inst})
            px = float(tk.get("data", [{}])[0].get("last", 0)) if tk.get("data") else 0
            if oi_ct and px:
                return {"exchange":"okx", "oi_usd": oi_ct * px}
    except Exception:
        pass
    return {"exchange":"okx", "oi_usd": None}

def _aggregate(symbols: List[str]) -> Dict[str, Any]:
    t = _now_ms()
    out = {}
    for sym in symbols:
        rows = []
        for fn in (_binance_oi, _bybit_oi, _okx_oi):
            rows.append(fn(sym))
        out[sym] = {"timestamp": t, "exchanges": rows,
                    "total_oi_usd": sum(x["oi_usd"] for x in rows if x.get("oi_usd")) or None}
        # store snapshot (optional)
        con = _conn()
        for r in rows:
            con.execute("INSERT INTO agg_oi_snapshots(symbol,time_ms,exchange,oi_contracts,oi_usd) VALUES(?,?,?,?,?)",
                        (sym, t, r["exchange"], None, r["oi_usd"]))
        con.commit(); con.close()
    return out

@router.get("/snapshot")
@router.post("/snapshot")
def snapshot(symbols: str = Query(..., description="Comma-separated, e.g. BTCUSDT,ETHUSDT")):
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    try:
        return _aggregate(syms)
    except Exception as e:
        return {"error": str(e), "symbols": syms}

@router.get("/oi")
def oi(symbol: str):
    try:
        return _aggregate([symbol]).get(symbol)
    except Exception as e:
        return {"error": str(e)}

@router.get("/oi_series")
def oi_series(symbol: str, bucket: str = "daily", days: int = 60):
    # roll snapshots by day for a quick chart
    from datetime import datetime, timezone, timedelta
    import pandas as pd
    now = int(datetime.now(timezone.utc).timestamp()*1000)
    since = now - days*24*3600*1000
    con = _conn()
    df = pd.read_sql_query(
        "SELECT time_ms, exchange, oi_usd FROM agg_oi_snapshots WHERE symbol=? AND time_ms>=?",
        con, params=(symbol, since))
    con.close()
    if df.empty:
        return {"series": []}
    df["t"] = (df["time_ms"]//(24*3600*1000))*(24*3600*1000)  # day bucket
    agg = df.groupby("t", as_index=False)["oi_usd"].sum(numeric_only=True)
    return {"series": [{"t": int(x.t), "oi_usd": float(x.oi_usd)} for x in agg.itertuples(index=False)]}
