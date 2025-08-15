# backend/services/ws_liq.py
from __future__ import annotations
import os, json, time, math
import threading
import traceback
import websockets
import asyncio
from datetime import datetime, timezone
from typing import Optional

from services.storage import conn

UTC = timezone.utc

# Control from env:
RUN_WS = os.getenv("RUN_WS", "1") == "1"  # set RUN_WS=0 on Streamlit so it never starts there
BIN_WS = os.getenv("BIN_WS", "wss://fstream.binance.com/stream?streams=!forceOrder@arr")

def _ensure_tables():
    c = conn()
    c.execute("""
    CREATE TABLE IF NOT EXISTS liq_events (
        ts_ms INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT,         -- BUY/SELL
        price REAL,
        qty REAL,
        notional REAL
    );
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_liq_ts ON liq_events(ts_ms);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_liq_sym_ts ON liq_events(symbol, ts_ms);")
    c.commit()

def _parse_binance_force_order(msg: dict) -> Optional[dict]:
    # Combined stream frame: {"stream":"!forceOrder@arr","data":{...}}
    d = msg.get("data") or msg
    o = d.get("o") or {}
    sym = o.get("s")
    if not sym:
        return None
    side = o.get("S")  # BUY / SELL
    # Prefer average price 'ap' (sometimes string), fallback to 'p'
    price = float(o.get("ap") or o.get("p") or 0) if (o.get("ap") or o.get("p")) else None
    qty = float(o.get("q") or o.get("z") or o.get("l") or 0) if (o.get("q") or o.get("z") or o.get("l")) else None
    ts = int(o.get("T") or d.get("E") or time.time() * 1000)
    if not price or not qty:
        return None
    notional = price * qty
    return {"ts_ms": ts, "symbol": sym, "side": side, "price": price, "qty": qty, "notional": notional}

async def _ws_loop(stop_event: threading.Event):
    _ensure_tables()
    backoff = 1.0
    while not stop_event.is_set():
        try:
            async with websockets.connect(BIN_WS, ping_interval=20, ping_timeout=20, max_size=2_000_000) as ws:
                backoff = 1.0
                while not stop_event.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    try:
                        msg = json.loads(raw)
                        row = _parse_binance_force_order(msg)
                        if not row:
                            continue
                        c = conn()
                        c.execute(
                            "INSERT INTO liq_events(ts_ms, symbol, side, price, qty, notional) VALUES(?,?,?,?,?,?)",
                            (row["ts_ms"], row["symbol"], row["side"], row["price"], row["qty"], row["notional"]),
                        )
                        c.commit()
                    except Exception:
                        # keep going on malformed frames
                        traceback.print_exc()
        except Exception:
            traceback.print_exc()
            # exponential backoff on connection failures
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

def start_liq_buffer():
    """
    Launch Binance !forceOrder@arr consumer in this process.
    Run only in the backend (RUN_WS=1).
    """
    stop_event = threading.Event()

    def _runner():
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        loop.run_until_complete(_ws_loop(stop_event))

    t = threading.Thread(target=_runner, name="liq-ws-thread", daemon=True)
    t.start()
    return stop_event
