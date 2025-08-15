from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from backend.services.ws_liq import RUN_WS, start_liq_buffer, get_heatmap

BACKEND_LOG_LEVEL = os.getenv("BACKEND_LOG_LEVEL", "info").lower()

app = FastAPI(title="Crypto Macro Suite – Backend")

# CORS so Streamlit (different host) can call us
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten if you want
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def jget(url: str, params: Optional[dict] = None, timeout: int = 20, retries: int = 2) -> Any:
    last_exc: Optional[str] = None
    for i in range(retries + 1):
        try:
            r = requests.get(url, params=params or {}, headers=UA, timeout=timeout)
            if r.status_code in (403, 418, 429, 451, 520):
                time.sleep(0.4 * (2 ** i))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = str(e)
            time.sleep(0.4 * (2 ** i))
    return {"_error": last_exc or "unknown", "_url": url}


@app.on_event("startup")
def _kick_ws():
    if RUN_WS:
        start_liq_buffer()


@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time() * 1000)}


# ---------------------------
# Derivatives (Binance proxy)
# ---------------------------
@app.get("/derivs/oi_hist")
def oi_hist(symbol: str = Query(..., description="e.g. BTCUSDT"),
            period: str = Query("1h", description="5m,15m,30m,1h,2h,4h,6h,12h,1d"),
            limit: int = Query(500, ge=1, le=1500)):
    url = "https://fapi.binance.com/futures/data/openInterestHist"
    return jget(url, {"symbol": symbol.upper(), "period": period, "limit": limit})


@app.get("/derivs/ls_ratio")
def ls_ratio(symbol: str = Query(...), period: str = Query("1h"), limit: int = Query(500, ge=1, le=1500)):
    url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
    return jget(url, {"symbol": symbol.upper(), "period": period, "limit": limit})


@app.get("/derivs/taker_ratio")
def taker_ratio(symbol: str = Query(...), period: str = Query("1h"), limit: int = Query(500, ge=1, le=1500)):
    url = "https://fapi.binance.com/futures/data/takerlongshortRatio"
    return jget(url, {"symbol": symbol.upper(), "period": period, "limit": limit})


# ---------------------------
# Simple aggregated OI
# ---------------------------
def _binance_oi_series(symbol: str, days: int) -> List[Dict[str, Any]]:
    # use daily period to match UI bucket
    js = jget("https://fapi.binance.com/futures/data/openInterestHist",
              {"symbol": symbol, "period": "1d", "limit": max(1, min(days, 365))})
    if isinstance(js, list):
        # Binance returns: [{ "sumOpenInterest": "...", "sumOpenInterestValue": "...", "timestamp": 1713916800000 }, ...]
        out = []
        for row in js:
            try:
                out.append({
                    "t": int(row.get("timestamp")),
                    "oi_usd": float(row.get("sumOpenInterestValue", 0.0)),
                    "oi_contracts": float(row.get("sumOpenInterest", 0.0)),
                    "exchange": "binance",
                })
            except Exception:
                pass
        return out
    return []


@app.get("/agg/oi")
def agg_oi(symbol: str = Query(..., description="e.g. BTCUSDT")):
    # current snapshot from daily series last point (Binance only to keep it free/simple)
    series = _binance_oi_series(symbol.upper(), days=3)
    val = series[-1]["oi_usd"] if series else 0.0
    return {
        "symbol": symbol.upper(),
        "exchanges": [
            {"exchange": "binance", "oi_usd": val}
        ],
        "total_oi_usd": val
    }


@app.get("/agg/oi_series")
def agg_oi_series(symbol: str = Query(...), bucket: str = Query("daily"), days: int = Query(60, ge=1, le=365)):
    # bucket is ignored (we always return daily in this free plan)
    series = _binance_oi_series(symbol.upper(), days=days)
    # return a compact series array
    out = [{"t": row["t"], "oi_usd": row["oi_usd"]} for row in series]
    return {"symbol": symbol.upper(), "series": out}


# ---------------------------
# Liquidations heatmap
# ---------------------------
@app.get("/liq/heatmap")
def liq_heatmap(symbol: str = Query("BTCUSDT"), minutes: int = Query(30, ge=1, le=240), bins: int = Query(50, ge=10, le=200)):
    return get_heatmap(symbol.upper(), minutes, bins)


# ---------------------------
# Macro (stub)
# ---------------------------
@app.get("/macro/series")
def macro_series(bucket: str = Query("daily"), days: int = Query(180, ge=1, le=1000)):
    # Your Streamlit UI already shows a help message when this returns empty.
    return {"bucket": bucket, "series": []}


# ---------------------------
# Missing endpoint you called earlier
# ---------------------------
@app.get("/agg/snapshot")
def agg_snapshot(symbols: str = Query(..., description="Comma-separated, e.g. BTCUSDT,ETHUSDT")):
    out = []
    for sym in [s.strip().upper() for s in symbols.split(",") if s.strip()]:
        snap = agg_oi(sym)
        out.append({"symbol": sym, "total_oi_usd": snap.get("total_oi_usd", 0.0)})
    return {"symbols": out}
