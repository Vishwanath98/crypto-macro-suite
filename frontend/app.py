# ============================
# frontend/app.py
# ============================
from __future__ import annotations
import os, time, json
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import requests
import streamlit as st

st.set_page_config(page_title="Crypto Macro (Free)", page_icon="📊", layout="wide")
UTC = timezone.utc

BACKEND = st.secrets.get("BACKEND_URL", os.environ.get("BACKEND_URL", ""))
if not BACKEND:
    st.warning("Set BACKEND_URL in Streamlit Secrets to enable backend-powered charts.")

@st.cache_data(show_spinner=False)
def jget(url: str, params: Optional[dict] = None, retries: int = 2):
    ua = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    last = None
    for i in range(retries+1):
        try:
            r = requests.get(url, params=params or {}, headers=ua, timeout=20)
            if r.status_code in (403,418,429,451):
                last = {"_error": f"http_{r.status_code}", "_url": url}; time.sleep(0.3*(2**i)); continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = {"_error": str(e), "_url": url}; time.sleep(0.3*(2**i))
    return last or {}

# ---------- UI ----------
st.title("📊 Crypto Macro Dashboard — Free APIs Only")
with st.sidebar:
    st.header("Settings")
    symbols = st.multiselect("Symbols", ["BTCUSDT","ETHUSDT"],["BTCUSDT","ETHUSDT"])
    deriv_period = st.selectbox("Derivs period", ["5m","15m","30m","1h","2h","4h","12h","1d"], index=3)
    liq_minutes = st.slider("Liq window (minutes)", 5, 120, 30)
    bins = st.slider("Heatmap bins", 20, 80, 50)
    st.caption("Backend URL: "+(BACKEND or "<not set>"))

# ---------- Tabs ----------
t_over, t_macro, t_derivs, t_liq = st.tabs(["Overview","Macro","Derivatives","Liquidations"])

# ---- Macro ----
with t_macro:
    st.subheader("Market Caps & BTC Dominance (from snapshots)")
    rng = st.select_slider("Range (days)", [30,90,180,365], value=180)
    js = jget(f"{BACKEND}/macro/series", {"bucket": "daily", "days": rng}) if BACKEND else {"series": []}
    df = pd.DataFrame(js.get("series", []))
    if df.empty:
        st.info("No macro snapshots yet. In Render, set a cron to call /macro/snapshot hourly/daily.")
    else:
        df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        c1,c2 = st.columns([2,1])
        with c1:
            st.plotly_chart(px.area(df, x="t", y=["btc","eth","alt"], title="BTC / ETH / ALTCAP"), use_container_width=True)
            st.plotly_chart(px.line(df, x="t", y=["total","volume"], title="Total Market Cap & Volume"), use_container_width=True)
        with c2:
            st.plotly_chart(px.line(df, x="t", y="btc_dom", title="BTC Dominance %"), use_container_width=True)

# ---- Derivatives ----
with t_derivs:
    st.subheader("Open Interest, Long/Short, Taker Flow (Binance public)")
    for sym in symbols:
        st.markdown(f"### {sym}")
        oi = jget(f"{BACKEND}/derivs/oi_hist", {"symbol": sym, "period": deriv_period, "limit": 500}) if BACKEND else []
        ls = jget(f"{BACKEND}/derivs/ls_ratio", {"symbol": sym, "period": deriv_period, "limit": 500}) if BACKEND else []
        tk = jget(f"{BACKEND}/derivs/taker_ratio", {"symbol": sym, "period": deriv_period, "limit": 500}) if BACKEND else []
        def tsify(d):
            df = pd.DataFrame(d)
            for k in ("timestamp","time","T"):
                if k in df: df["t"] = pd.to_datetime(df[k], unit="ms", utc=True)
            return df
        dfo, dfl, dft = (tsify(oi), tsify(ls), tsify(tk))
        if not dfo.empty:
            st.plotly_chart(px.area(dfo, x="t", y="sumOpenInterestValue", title=f"{sym} OI Notional (USD)"), use_container_width=True)
        else:
            st.info("No OI hist (Binance blocked or empty).")
        if not dfl.empty and {"longAccount","shortAccount"}.issubset(dfl.columns):
            st.plotly_chart(px.line(dfl, x="t", y=["longAccount","shortAccount"], title=f"{sym} Global Long/Short Account Ratio"), use_container_width=True)
        if not dft.empty and {"buyVol","sellVol"}.issubset(dft.columns):
            st.plotly_chart(px.line(dft, x="t", y=["buyVol","sellVol"], title=f"{sym} Taker Buy/Sell Volume"), use_container_width=True)

    st.divider()
    st.subheader("Aggregated OI (Binance + Bybit + OKX)")
    for sym in symbols:
        js = jget(f"{BACKEND}/agg/oi", {"symbol": sym}) if BACKEND else {}
        rows = js.get("exchanges", [])
        if rows:
            st.dataframe(pd.DataFrame(rows))
        series = jget(f"{BACKEND}/agg/oi_series", {"symbol": sym, "bucket": "daily", "days": 60})
        srf = pd.DataFrame(series.get("series", []))
        if not srf.empty:
            srf["t"] = pd.to_datetime(srf["t"], unit="ms", utc=True)
            st.plotly_chart(px.line(srf, x="t", y="oi_usd", title=f"{sym} Aggregated OI Notional (USD)"), use_container_width=True)

# ---- Liquidations ----
with t_liq:
    st.subheader("Live Liquidations Heatmap (Binance !forceOrder@arr → backend buffer)")
    sym = st.selectbox("Symbol", symbols, index=0)
    js = jget(f"{BACKEND}/liq/heatmap", {"symbol": sym, "minutes": liq_minutes, "bins": bins}) if BACKEND else {"x":[],"y":[],"z":[]}
    if not js.get("x"):
        st.info("Waiting for liquidation prints (or backend not running yet)...")
    else:
        fig = go.Figure(data=go.Heatmap(z=js["z"], x=js["x"], y=pd.to_datetime(js["y"], unit="ms", utc=True)))
        fig.update_layout(height=420, margin=dict(l=10,r=10,t=30,b=10), title=f"{sym} – Liq Notional Heatmap")
        st.plotly_chart(fig, use_container_width=True)

# ---- Overview ----
with t_over:
    st.write("This page uses only free/public APIs and your backend to avoid Cloud WAF blocks. Use the sidebar to switch symbols and the tabs to dive in.")