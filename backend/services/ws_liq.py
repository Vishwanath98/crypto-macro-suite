import asyncio
import json
import os
import time
from collections import defaultdict, deque
from typing import Deque, Dict, Tuple, List

import websockets

RUN_WS = os.getenv("RUN_WS", "1") == "1"  # default True for backend

# store last N minutes of prints per symbol
# each item: (ts_ms: int, price: float, notional_usd: float, side: str)
_BUFS: Dict[str, Deque[Tuple[int, float, float, str]]] = defaultdict(lambda: deque(maxlen=20000))

_ws_task_started = False

WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"


async def _consume_force_orders():
    """
    Connects to Binance futures liquidation feed and appends prints into memory.
    Reconnects on errors with backoff.
    """
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=15, ping_timeout=20) as ws:
                backoff = 1.0
                while True:
                    raw = await ws.recv()
                    # The '!forceOrder@arr' stream delivers either a single event or an array of events.
                    data = json.loads(raw)
                    events = data if isinstance(data, list) else [data]
                    now_ms = int(time.time() * 1000)

                    for ev in events:
                        # expected shape: {"e":"forceOrder","E":..,"o":{ ... }}
                        o = (ev or {}).get("o") or {}
                        sym = o.get("s")
                        if not sym:
                            continue
                        # choose price: average (ap) if present, else order price (p)
                        ap = o.get("ap", "0")
                        p = o.get("p", "0")
                        price = float(ap) if ap and ap != "0" else float(p or 0.0)
                        qty = float(o.get("q", "0") or 0.0)
                        side = o.get("S", "UNKNOWN")
                        ts = int(o.get("T") or ev.get("E") or now_ms)
                        notional = price * qty
                        if price <= 0 or qty <= 0:
                            continue
                        buf = _BUFS[sym]
                        buf.append((ts, price, notional, side))
        except Exception:
            # brief backoff then reconnect
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


def start_liq_buffer():
    """
    Launch the websocket consumer once per process in a private event loop.
    Safe to call multiple times; it will no-op after first start.
    """
    global _ws_task_started
    if _ws_task_started:
        return
    _ws_task_started = True

    def _runner():
        try:
            asyncio.run(_consume_force_orders())
        except Exception:
            # if the loop ever exits, let it die silently; Render will restart the dyno
            pass

    import threading
    t = threading.Thread(target=_runner, daemon=True, name="liq-ws-consumer")
    t.start()


def _now_ms() -> int:
    return int(time.time() * 1000)


def get_heatmap(symbol: str, minutes: int, bins: int):
    """
    Build a simple time x price heatmap of liquidation notional (USD)
    from the in-memory buffer.
    Returns dict: {"x": [price bins], "y": [bucket start ms], "z": 2D list}
    """
    symbol = symbol.upper()
    if symbol not in _BUFS:
        return {"x": [], "y": [], "z": []}

    end_ms = _now_ms()
    start_ms = end_ms - minutes * 60_000

    # slice relevant window
    items = [it for it in _BUFS[symbol] if it[0] >= start_ms]
    if not items:
        return {"x": [], "y": [], "z": []}

    # price bins
    lo = min(p for _, p, _, _ in items)
    hi = max(p for _, p, _, _ in items)
    if hi <= lo:
        return {"x": [], "y": [], "z": []}

    # widen a touch so edges aren't cramped
    pad = (hi - lo) * 0.01
    lo -= pad
    hi += pad

    # build axes
    bin_w = (hi - lo) / max(1, bins)
    x_bins = [lo + i * bin_w for i in range(bins)]
    # 1-minute buckets on y
    y_slots = list(range(minutes))
    y_ms = [start_ms + i * 60_000 for i in y_slots]

    # z matrix (time buckets x price bins)
    z = [[0.0 for _ in range(bins)] for _ in y_slots]

    # fill
    for ts, price, notional, _side in items:
        yi = min(max((ts - start_ms) // 60_000, 0), minutes - 1)
        xi = int((price - lo) // bin_w)
        if xi < 0:
            xi = 0
        elif xi >= bins:
            xi = bins - 1
        z[yi][xi] += notional

    return {"x": x_bins, "y": y_ms, "z": z}
