# ======================
# backend/routers/liq.py
# ======================
from __future__ import annotations
import asyncio, json, time, os
from collections import deque, defaultdict
from typing import Deque, Dict

import websockets
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/liq", tags=["liquidations"])

BIN_WS = os.getenv("BIN_WS", "wss://fstream.binance.com/stream?streams=!forceOrder@arr")
BUFFER_SECS = int(os.getenv("LIQ_BUFFER_SECS", "3600"))  # keep last hour in memory

# Ring buffer: list of (ts_ms, symbol, side, price, qty, notional)
BUF: Deque[tuple] = deque(maxlen=200000)

async def liq_consumer():
    while True:
        try:
            async with websockets.connect(BIN_WS, ping_interval=20, ping_timeout=10) as ws:
                async for msg in ws:
                    try:
                        d = json.loads(msg)
                        o = (d.get("data") or {}).get("o", {})
                        s = o.get("s")
                        side = o.get("S")
                        p = float(o.get("p") or 0)
                        q = float(o.get("q") or 0)
                        t = int(o.get("T") or time.time()*1000)
                        if s and p > 0 and q > 0:
                            BUF.append((t, s.upper(), side, p, q, p*q))
                    except Exception:
                        pass
        except Exception:
            await asyncio.sleep(1)

@router.on_event("startup")
async def start_bg():
    asyncio.create_task(liq_consumer())

@router.get("/heatmap")
def heatmap(symbol: str = "BTCUSDT", minutes: int = 30, bins: int = 50):
    cutoff = int((time.time() - minutes*60) * 1000)
    rows = [r for r in list(BUF) if r[0] >= cutoff and r[1] == symbol.upper()]
    if not rows:
        return {"x": [], "y": [], "z": []}
    # build minute buckets (y) vs price bins (x)
    import numpy as np
    import pandas as pd
    df = pd.DataFrame(rows, columns=["ts","symbol","side","price","qty","notional"])
    df["minute"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.floor("min")
    pmin, pmax = float(df["price"].min()), float(df["price"].max())
    if pmin == pmax:
        pmin, pmax = pmin*0.99, pmax*1.01
    edges = np.linspace(pmin, pmax, bins)
    df["bin"] = pd.cut(df["price"], bins=edges, include_lowest=True)
    pv = df.pivot_table(index="minute", columns="bin", values="notional", aggfunc="sum").fillna(0)
    x = [str(c) for c in pv.columns]
    y = [int(ts.value/10**6) for ts in pv.index]  # ms
    z = pv.values.tolist()
    return {"x": x, "y": y, "z": z}
