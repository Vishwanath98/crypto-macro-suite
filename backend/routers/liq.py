from fastapi import APIRouter, Query
from typing import Dict, Any
from datetime import datetime, timedelta, timezone
import numpy as np
import sqlite3
from services.db import _conn

router = APIRouter()

@router.get("/status")
def status(minutes: int = 60):
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    since = now - minutes*60*1000
    con = _conn()
    cur = con.execute("SELECT COUNT(*), MAX(time_ms) FROM liq_events WHERE time_ms>=?", (since,))
    cnt, last = cur.fetchone()
    con.close()
    return {"events_last_min": cnt, "last_ms": last}

@router.get("/heatmap")
def heatmap(symbol: str = Query(...), minutes: int = 30, bins: int = 50) -> Dict[str, Any]:
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    start = now - minutes*60*1000
    con = _conn()
    cur = con.execute("""SELECT time_ms, price, quote_qty FROM liq_events
                         WHERE symbol=? AND time_ms BETWEEN ? AND ?""",
                      (symbol, start, now))
    rows = cur.fetchall()
    con.close()
    if not rows:
        return {"x": [], "y": [], "z": []}

    import pandas as pd
    df = pd.DataFrame(rows, columns=["t","price","q"])
    # time buckets on Y, price buckets on X, Z = notional sum
    tbins = np.linspace(start, now, num=min(bins, max(3, minutes)))  # about 1 bucket per minute
    pbins = np.linspace(df["price"].min(), df["price"].max(), num=bins)
    H, xedges, yedges = np.histogram2d(df["price"], df["t"], bins=[pbins, tbins], weights=df["q"].fillna(0).values)
    # return centers to plot cleanly
    x = ((xedges[:-1] + xedges[1:]) / 2.0).tolist()
    y = ((yedges[:-1] + yedges[1:]) / 2.0).astype("int64").tolist()
    z = H.T.tolist()
    return {"x": x, "y": y, "z": z}
