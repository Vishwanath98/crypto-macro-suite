# Streamlit Frontend — Crypto Macro Dashboard with WebSocket Liquidation Heatmap
from __future__ import annotations
import json, threading, time, os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except Exception:
    HAS_AUTOREFRESH = False

try:
    import websocket  # websocket-client
    HAS_WS = True
except Exception:
    HAS_WS = False

UTC = timezone.utc
BINANCE_FAPI = "https://fapi.binance.com"
COINGECKO_API = "https://api.coingecko.com/api/v3"
FNG_API = "https://api.alternative.me/fng/"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]

st.set_page_config(page_title="Crypto Macro Dashboard", page_icon="📈", layout="wide", initial_sidebar_state="expanded")

@st.cache_data(show_spinner=False)
def _fetch_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, ttl: int = 60) -> dict:
    r = requests.get(url, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()

def human_fmt(num) -> str:
    try:
        num = float(num)
    except Exception:
        return str(num)
    for unit in ("", "K", "M", "B", "T"):
        if abs(num) < 1000:
            return f"{num:,.2f}{unit}"
        num /= 1000.0
    return f"{num:.2f}P"

def coingecko_global() -> Dict:
    return _fetch_json(f"{COINGECKO_API}/global", ttl=60).get("data", {})

def coingecko_coin_snap(ids: List[str]) -> pd.DataFrame:
    params = {"vs_currency": "usd","ids": ",".join(ids),"order": "market_cap_desc","per_page": len(ids) or 2,
              "page": 1,"sparkline": "false","price_change_percentage": "24h"}
    return pd.DataFrame(_fetch_json(f"{COINGECKO_API}/coins/markets", params=params, ttl=60))

def binance_klines(symbol: str, interval: str, start: int, end: int, limit: int = 1000) -> pd.DataFrame:
    url = f"{BINANCE_FAPI}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit, "startTime": start, "endTime": end}
    data = _fetch_json(url, params=params, ttl=0)
    cols = ["open_time","open","high","low","close","volume","close_time","quote_asset_volume",
            "number_of_trades","taker_buy_base","taker_buy_quote","ignore"]
    df = pd.DataFrame(data, columns=cols)
    for c in ["open","high","low","close","volume","quote_asset_volume","taker_buy_base","taker_buy_quote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df

def binance_open_interest_hist(symbol: str, period: str = "1h", start: Optional[int] = None, end: Optional[int] = None) -> pd.DataFrame:
    url = f"{BINANCE_FAPI}/futures/data/openInterestHist"
    params = {"symbol": symbol, "period": period, "limit": 500}
    if start: params["startTime"] = start
    if end: params["endTime"] = end
    data = _fetch_json(url, params=params, ttl=0)
    df = pd.DataFrame(data)
    if df.empty: return df
    for c in ["sumOpenInterestValue", "sumOpenInterest"]:
        if c in df: df[c] = pd.to_numeric(df[c], errors="coerce")
    if "timestamp" in df: df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df

def binance_open_interest_now(symbol: str) -> Optional[float]:
    try:
        js = _fetch_json(f"{BINANCE_FAPI}/fapi/v1/openInterest", params={"symbol": symbol}, ttl=0)
        return float(js.get("openInterest"))
    except Exception:
        return None

def binance_funding_rate(symbol: str) -> Optional[float]:
    try:
        js = _fetch_json(f"{BINANCE_FAPI}/fapi/v1/fundingRate", params={"symbol": symbol, "limit": 1}, ttl=0)
        return float(js[0].get("fundingRate", 0)) if isinstance(js, list) and js else None
    except Exception:
        return None

def fear_and_greed() -> Dict:
    try:
        js = _fetch_json(FNG_API, params={"limit":1, "date_format":"us"}, ttl=1800)
        d = js["data"][0]
        return {"value": int(d["value"]), "classification": d["value_classification"], "timestamp": d["timestamp"]}
    except Exception:
        return {"value": None, "classification": "N/A", "timestamp": None}

def sidebar_controls() -> dict:
    st.sidebar.header("⚙️ Controls")
    symbols = st.sidebar.multiselect("Futures symbols", DEFAULT_SYMBOLS, default=DEFAULT_SYMBOLS)
    interval = st.sidebar.selectbox("Price/OI interval", ["5m","15m","1h","4h","1d"], index=2)
    lookback = st.sidebar.selectbox("Lookback", ["1D","3D","7D","30D"], index=2)
    refresh_sec = st.sidebar.slider("Auto-refresh (sec)", 0, 300, 30)

    st.sidebar.divider()
    st.sidebar.subheader("Live Liquidations")
    enable_ws = st.sidebar.checkbox("Enable Binance WebSocket", value=True, help="Live force orders stream")
    liq_window = st.sidebar.slider("Heatmap window (minutes)", 5, 120, 30)
    st.session_state["liq_window_min"] = liq_window

    st.sidebar.divider()
    st.sidebar.subheader("Backend (OI aggregator)")
    backend_url = st.sidebar.text_input("Backend base URL", value=os.environ.get("BACKEND_URL", ""), help="If set, OI aggregated across exchanges will be pulled from FastAPI backend.")

    st.sidebar.divider()
    st.sidebar.subheader("Alerts")
    alert_fng = st.sidebar.slider("Fear & Greed ≤", 0, 100, 25)
    alert_oi_jump = st.sidebar.slider("OI 1h jump ≥ %", 0, 50, 10)

    if HAS_AUTOREFRESH and refresh_sec > 0:
        st.sidebar.caption("🔄 Auto-refresh enabled")

    return dict(symbols=symbols, interval=interval, lookback=lookback, refresh_sec=refresh_sec,
                enable_ws=enable_ws, backend_url=backend_url, alert_fng=alert_fng, alert_oi_jump=alert_oi_jump)

def lookback_to_ms(lookback: str):
    now = datetime.now(UTC)
    start = now - (dict(**{"1D":timedelta(days=1), "3D":timedelta(days=3), "7D":timedelta(days=7)}).get(lookback) or timedelta(days=30))
    return int(start.timestamp()*1000), int(now.timestamp()*1000)

def _ws_thread(symbols_lower: List[str]):
    streams = "/".join(f"{s}@forceOrder" for s in symbols_lower)
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    def on_message(ws, message):
        try:
            d = json.loads(message)
            payload = d.get("data") or d
            o = payload.get("o", {})
            sym = (o.get("s") or payload.get("s") or "").upper()
            side = o.get("S") or payload.get("S")
            p = float(o.get("p") or o.get("ap") or 0)
            q = float(o.get("q") or o.get("l") or 0)
            ts = int(o.get("T") or payload.get("E") or time.time()*1000)
            if p <= 0 or q <= 0:
                return
            row = dict(ts=pd.to_datetime(ts, unit="ms", utc=True), symbol=sym, price=p, qty=q, side=side, notional=p*q)
            buf = st.session_state.setdefault("liq_buffer", [])
            buf.append(row)
            cutoff = pd.Timestamp.utcnow() - pd.Timedelta(minutes=st.session_state.get("liq_window_min", 30))
            st.session_state["liq_buffer"] = [r for r in buf if r["ts"] >= cutoff]
        except Exception:
            pass
    ws = websocket.WebSocketApp(url, on_message=on_message)
    ws.run_forever(ping_interval=20, ping_timeout=10)

def ensure_ws(symbols: List[str], enable: bool):
    if not enable or not HAS_WS:
        return
    if "ws_running" in st.session_state:
        return
    t = threading.Thread(target=_ws_thread, args=([s.lower() for s in symbols],), daemon=True)
    t.start()
    st.session_state["ws_running"] = True

def render_liq_heatmap(selected_symbol: str):
    buf = st.session_state.get("liq_buffer", [])
    if not buf:
        st.info("Waiting for live liquidation prints…")
        return
    df = pd.DataFrame(buf)
    df = df[df["symbol"] == selected_symbol]
    if df.empty:
        st.info("No prints for this symbol yet.")
        return
    df["minute"] = df["ts"].dt.floor("min")
    pmin, pmax = float(df["price"].min()), float(df["price"].max())
    if pmin == pmax:
        pmin, pmax = pmin * 0.99, pmax * 1.01
    bins = np.linspace(pmin, pmax, 50)
    df["price_bin"] = pd.cut(df["price"], bins=bins, include_lowest=True)
    pv = df.pivot_table(index="minute", columns="price_bin", values="notional", aggfunc="sum").fillna(0)
    fig = go.Figure(data=go.Heatmap(z=pv.values, x=[str(c) for c in pv.columns], y=pv.index, coloraxis="coloraxis"))
    fig.update_layout(title=f"{selected_symbol} — Live Liquidations Heatmap (sum notional)", height=360, margin=dict(l=10,r=10,t=40,b=10), coloraxis=dict(colorbar=dict(title="Notional")))
    st.plotly_chart(fig, use_container_width=True)

def kpi_card(label: str, value: str, delta: Optional[str] = None):
    st.metric(label, value, delta=delta)

def render_global_kpis():
    g = coingecko_global()
    total_mcap = (g.get("total_market_cap") or {}).get("usd")
    total_vol = (g.get("total_volume") or {}).get("usd")
    btc_dom = (g.get("market_cap_percentage") or {}).get("btc")
    snap = coingecko_coin_snap(["bitcoin","ethereum"]) if total_mcap else pd.DataFrame()
    btc_mcap = snap.loc[snap["id"]=="bitcoin","market_cap"].values[0] if not snap.empty else None
    eth_mcap = snap.loc[snap["id"]=="ethereum","market_cap"].values[0] if not snap.empty else None
    alt_mcap = (total_mcap - (btc_mcap or 0) - (eth_mcap or 0)) if total_mcap and btc_mcap and eth_mcap else None
    c1,c2,c3,c4,c5 = st.columns(5)
    with c1: kpi_card("Total Market Cap", f"${human_fmt(total_mcap) if total_mcap else '—'}")
    with c2: kpi_card("BTC Dominance", f"{btc_dom:.2f}%" if btc_dom is not None else "—")
    with c3: kpi_card("Alt Mkt Cap (ex BTC/ETH)", f"${human_fmt(alt_mcap) if alt_mcap else '—'}")
    with c4: kpi_card("24h Volume", f"${human_fmt(total_vol) if total_vol else '—'}")
    fng = fear_and_greed()
    with c5: kpi_card("Fear & Greed", f"{fng.get('value','—')} ({fng.get('classification','N/A')})")

def plot_price(df: pd.DataFrame, symbol: str):
    if df.empty:
        st.info(f"No price data for {symbol}.")
        return
    fig = go.Figure(go.Candlestick(x=df["open_time"], open=df["open"], high=df["high"], low=df["low"], close=df["close"]))
    fig.update_layout(title=f"{symbol} Price", height=420, margin=dict(l=10,r=10,t=40,b=10))
    st.plotly_chart(fig, use_container_width=True)

def plot_volume(df: pd.DataFrame, symbol: str):
    if df.empty: return
    fig = px.bar(df, x="close_time", y="quote_asset_volume", title=f"{symbol} Quote Volume")
    fig.update_layout(height=220, margin=dict(l=10,r=10,t=40,b=10))
    st.plotly_chart(fig, use_container_width=True)

def plot_oi(df: pd.DataFrame, symbol: str):
    if df.empty:
        st.info(f"No OI hist for {symbol}.")
        return
    ts = df.get("timestamp")
    y = df.get("sumOpenInterestValue") or df.get("sumOpenInterest")
    fig = go.Figure(go.Scatter(x=ts, y=y, mode="lines"))
    fig.update_layout(title=f"{symbol} Open Interest (Binance hist)", height=280, margin=dict(l=10,r=10,t=40,b=10))
    st.plotly_chart(fig, use_container_width=True)

def backend_oi_table(backend_url: str, symbol: str):
    try:
        js = _fetch_json(f"{backend_url.rstrip('/')}/oi/{symbol}", ttl=0)
    except Exception as e:
        st.warning(f"Backend error for {symbol}: {e}")
        return
    if not isinstance(js, dict) or not js.get("exchanges"):
        st.info("No aggregated OI available.")
        return
    rows = []
    for ex in js["exchanges"]:
        rows.append({"exchange": ex["name"], "oi_value_usd": ex["oi_value_usd"], "timestamp": ex["timestamp"]})
    rows.append({"exchange": "TOTAL", "oi_value_usd": js.get("total_oi_usd"), "timestamp": js.get("as_of")})
    st.dataframe(pd.DataFrame(rows))

def main():
    st.title("📈 Crypto Macro Dashboard — Live Liqs + OI Aggregator")
    st.caption("Live liquidation heatmap, price/volume/OI, and optional multi-exchange OI via FastAPI backend.")
    cfg = sidebar_controls()
    if HAS_AUTOREFRESH and cfg["refresh_sec"] > 0:
        st_autorefresh(interval=cfg["refresh_sec"] * 1000, key="auto_refresh")
    start_ms, end_ms = lookback_to_ms(cfg["lookback"])
    render_global_kpis()
    st.divider()
    ensure_ws(cfg["symbols"], cfg["enable_ws"])
    for sym in cfg["symbols"]:
        st.subheader(sym)
        left, right = st.columns([2.2, 1])
        with left:
            df_p = binance_klines(sym, cfg["interval"], start_ms, end_ms)
            plot_price(df_p, sym)
            plot_volume(df_p, sym)
        with right:
            df_oi = binance_open_interest_hist(sym, period=cfg["interval"] if cfg["interval"] in ["5m","15m","30m","1h","2h","4h","6h","12h","1d"] else "1h", start=start_ms, end=end_ms)
            plot_oi(df_oi, sym)
            oi_now = binance_open_interest_now(sym)
            fr = binance_funding_rate(sym)
            k1,k2 = st.columns(2)
            with k1: kpi_card("Open Interest (contracts)", human_fmt(oi_now) if oi_now is not None else "—")
            with k2: kpi_card("Funding (last)", f"{fr*100:.4f}%" if fr is not None else "—")
        render_liq_heatmap(sym)
        if cfg["backend_url"]:
            st.markdown("**Aggregated OI (via backend):**")
            backend_oi_table(cfg["backend_url"], sym)
        st.divider()
    st.subheader("🔔 Alerts")
    msgs = []
    fng = fear_and_greed()
    if fng.get("value") is not None and fng["value"] <= cfg["alert_fng"]:
        msgs.append(f"F&G {fng['value']} (≤ {cfg['alert_fng']})")
    if msgs:
        for m in msgs: st.error(m)
    else:
        st.success("No alerts triggered.")
    st.caption("Sources: CoinGecko (global), Binance (price/OI/funding + live liqs). Backend can add Bybit/OKX/Deribit OI aggregation + persistence.")

if __name__ == "__main__":
    main()
