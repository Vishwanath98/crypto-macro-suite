import os, json, time, threading
from datetime import datetime, timezone
import websocket  # pip install websocket-client
from .db import _conn

RUN_WS = os.getenv("RUN_WS", "1") == "1"      # default: on
WS_URL = os.getenv("BINANCE_LIQ_WS",
                   "wss://fstream.binance.com/stream?streams=!forceOrder@arr")
_worker_started = False

def start_liq_buffer():
    global _worker_started
    if _worker_started:
        return
    _worker_started = True
  
def _save_event(ev):
    # ev is the "o" payload from Binance forceOrder
    try:
        sym = ev.get("s")
        side = ev.get("S")  # "SELL" liquidation => longs got liq'd
        price = float(ev.get("p"))
        qty   = float(ev.get("q", 0))
        qnot  = float(ev.get("ap", 0)) * qty if ev.get("ap") else price*qty
        t     = int(ev.get("T") or ev.get("E") or int(time.time()*1000))
        con = _conn()
        con.execute("INSERT INTO liq_events(symbol,time_ms,price,side,qty,quote_qty) VALUES (?,?,?,?,?,?)",
                    (sym, t, price, side, qty, qnot))
        con.commit(); con.close()
    except Exception:
        pass

def start_liq_buffer():
    # simple forever loop with auto-reconnect
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_message=lambda ws, msg: _on_msg(msg),
                on_error=lambda ws, err: None,
                on_close=lambda ws, code, msg: None)
            ws.run_forever(ping_interval=15, ping_timeout=10)
        except Exception:
            time.sleep(2)  # backoff

def _on_msg(msg):
    try:
        js = json.loads(msg)
        # combined stream payload: {"stream":"!forceOrder@arr","data":[{...},{...}]}
        data = js.get("data", js)  # sometimes library gives dict or list
        arr = data if isinstance(data, list) else data.get("o") and [data] or []
        for item in arr:
            # In per-event stream, payload is {"e":"forceOrder","E":...,"o":{...}}
            if "o" in item:
                _save_event(item["o"])
            else:
                # Some gateways deliver directly the "o" object
                _save_event(item)
    except Exception:
        pass
