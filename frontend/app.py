# ============================
# frontend/app.py
# ============================
from __future__ import annotations
import os, time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import requests
import streamlit as st

st.set_page_config(page_title="Crypto Macro (Free)", page_icon="📊", layout="wide")
UTC = timezone.utc

# --------------------------
# Backend base URL
# --------------------------
# Set BACKEND_URL in Streamlit Secrets or env. We strip trailing slashes to avoid '//' when building paths.
BACKEND = (st.secrets.get("BACKEND_URL", os.environ.get("BACKEND_URL", "")) or "").rstrip("/")

# --------------------------
# HTTP helpers (cached GET)
# --------------------------
@st.cache_data(show_spinner=False)
def jget_abs(url: str, params: Optional[dict] = None, retries: int = 2):
    """GET JSON with simple retries + user-agent (helps with public APIs)."""
    ua = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    last = None
    for i in range(retries + 1):
        try:
            r = requests.get(url, params=params or {}, headers=ua, timeout=20)
            if r.status_code in (403, 418, 429, 451):
                last = {"_error": f"http_{r.status_code}", "_url": url}
                time.sleep(0.3 * (2 ** i))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = {"_error": str(e), "_url": url}
            time.sleep(0.3 * (2 ** i))
    return last or {}

def api_get(path: str, params: Optional[dict] = None, retries: int = 2):
    """
    Convenience wrapper to GET from the backend, e.g. api_get("/macro/series", {...})
    Returns {} on missing BACKEND to simplify callers.
    """
    if not BACKEND:
        return {"_error": "no_backend", "_url": path}
    url = f"{BACKEND}{path if path.startswith('/') else '/' + path}"
    return jget_abs(url, params=params, retries=retries)

# Simple cache-clear button
def clear_caches():
    jget_abs.clear()

# --------------------------
# Spot OHLC (free fallbacks)
# --------------------------
def spot_ohlc(symbol: str, interval: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """
    Robust free OHLC with fallbacks:
      1) Binance spot  /api/v3/klines
      2) Kraken        /0/public/OHLC   (XBTUSD / ETHUSD)
      3) Bitstamp      /api/v2/ohlc     (btcusdt / ethusdt)
    Returns DataFrame with columns: open_time, open, high, low, close, volume, close_time
    """
    pair = "BTCUSDT" if symbol.upper().startswith("BTC") else "ETHUSDT"
    kr_pair = "XBTUSD" if symbol.upper().startswith("BTC") else "ETHUSD"
    bs_pair = "btcusdt" if symbol.upper().startswith("BTC") else "ethusdt"

    gran = {"5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400, "12h": 43200, "1d": 86400}
    g = gran.get(interval, 3600)

    # 1) Binance spot
    try:
        js = jget_abs(
            "https://api.binance.com/api/v3/klines",
            {"symbol": pair, "interval": interval, "startTime": start_ms, "endTime": end_ms, "limit": 1000},
        )
        if isinstance(js, list) and js:
            cols = ["open_time","open","high","low","close","volume","close_time","qav","trades","tbb","tbq","ignore"]
            df = pd.DataFrame(js, columns=cols)
            for c in ["open","high","low","close","volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
            return df[["open_time","open","high","low","close","volume","close_time"]]
    except Exception:
        pass

    # 2) Kraken
    try:
        js = jget_abs("https://api.kraken.com/0/public/OHLC", {"pair": kr_pair, "interval": g // 60})
        data = (js.get("result") or {}).get(kr_pair, [])
        if isinstance(data, list) and data:
            # [time, open, high, low, close, vwap, volume, count]
            df = pd.DataFrame(data, columns=["t","open","high","low","close","vwap","volume","count"])
            for c in ["open","high","low","close","volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["open_time"] = pd.to_datetime(df["t"], unit="s", utc=True)
            df["close_time"] = df["open_time"] + pd.to_timedelta(g, unit="s")
            return df[["open_time","open","high","low","close","volume","close_time"]]
    except Exception:
        pass

    # 3) Bitstamp
    try:
        js = jget_abs(f"https://www.bitstamp.net/api/v2/ohlc/{bs_pair}/", {"step": g, "limit": 1000})
        data = (js.get("data") or {}).get("ohlc", [])
        if isinstance(data, list) and data:
            df = pd.DataFrame(data)
            for c in ["open","high","low","close","volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["open_time"] = pd.to_datetime(df["timestamp"].astype(int), unit="s", utc=True)
            df["close_time"] = df["open_time"] + pd.to_timedelta(g, unit="s")
            return df[["open_time","open","high","low","close","volume","close_time"]]
    except Exception:
        pass

    return pd.DataFrame(columns=["open_time","open","high","low","close","volume","close_time"])

# ---------- UI ----------
st.title("📊 Crypto Macro Dashboard — Free APIs Only")

with st.sidebar:
    st.header("Settings")
    symbols = st.multiselect("Symbols", ["BTCUSDT", "ETHUSDT"], ["BTCUSDT", "ETHUSDT"])
    deriv_period = st.selectbox(
        "Derivs period", ["5m", "15m", "30m", "1h", "2h", "4h", "12h", "1d"], index=3
    )
    liq_minutes = st.slider("Liq window (minutes)", 5, 120, 30)
    bins = st.slider("Heatmap bins", 20, 80, 50)

    st.caption("Backend URL: " + (BACKEND or "<not set>"))
    colA, colB = st.columns(2)
    with colA:
        if st.button("Clear cache", use_container_width=True):
            clear_caches()
            st.experimental_rerun()
    with colB:
        pass

    # Backend health ping (if configured)
    if BACKEND:
        try:
            t0 = time.perf_counter()
            h = api_get("/health")  # should be cheap and public
            dt = (time.perf_counter() - t0) * 1000
            if isinstance(h, dict) or isinstance(h, list) or h:
                st.success(f"Backend: OK ({dt:.0f} ms)")
            else:
                st.warning(f"Backend health returned empty ({dt:.0f} ms)")
        except Exception as e:
            st.error(f"Backend health error: {e}")
    else:
        st.warning("Set BACKEND_URL in Streamlit Secrets to enable backend-powered charts.")

# ---------- Tabs ----------
t_over, t_macro, t_derivs, t_liq = st.tabs(["Overview", "Macro", "Derivatives", "Liquidations"])

# ---- Macro ----
with t_macro:
    st.subheader("Market Caps & BTC Dominance (from snapshots)")
    rng = st.select_slider("Range (days)", [30, 90, 180, 365], value=180)
    js = api_get("/macro/series", {"bucket": "daily", "days": rng}) if BACKEND else {"series": []}
    df = pd.DataFrame(js.get("series", []))
    if df.empty:
        if js.get("_error") == "no_backend":
            st.info("Backend not configured. Set BACKEND_URL in Secrets.")
        else:
            st.info("No macro snapshots yet. On your backend, schedule /macro/snapshot.")
    else:
        df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        c1, c2 = st.columns([2, 1])
        with c1:
            st.plotly_chart(
                px.area(df, x="t", y=["btc", "eth", "alt"], title="BTC / ETH / Altcoin Market Cap (USD)"),
                use_container_width=True,
            )
            st.plotly_chart(
                px.line(df, x="t", y=["total", "volume"], title="Total Market Cap & Volume (USD)"),
                use_container_width=True,
            )
        with c2:
            st.plotly_chart(px.line(df, x="t", y="btc_dom", title="BTC Dominance (%)"), use_container_width=True)

# ---- Derivatives (spot price + OI/ratios + aggregated OI) ----
with t_derivs:
    st.subheader("Spot Price, OI, Long/Short, Taker Flow (Free Public Endpoints via Backend)")

    def tsify(d):
        df = pd.DataFrame(d)
        if df.empty:
            return df
        for k in ("timestamp", "time", "T", "t"):
            if k in df:
                # assume ms
                df["t"] = pd.to_datetime(df[k], unit="ms", utc=True)
                break
        return df

    for sym in symbols:
        st.markdown(f"### {sym}")

        # --- Spot price/volume (fallback: Binance → Kraken → Bitstamp) ---
        lookback = st.selectbox(
            f"Price lookback for {sym}", ["7D", "30D"], index=0, key=f"lb_{sym}"
        )
        delta = {"7D": timedelta(days=7), "30D": timedelta(days=30)}[lookback]
        now = datetime.now(UTC)
        start_ms = int((now - delta).timestamp() * 1000)
        end_ms = int(now.timestamp() * 1000)

        df_p = spot_ohlc(sym, deriv_period, start_ms, end_ms)
        if not df_p.empty:
            # Candle chart
            fig = go.Figure(
                go.Candlestick(
                    x=df_p["open_time"],
                    open=df_p["open"],
                    high=df_p["high"],
                    low=df_p["low"],
                    close=df_p["close"],
                )
            )
            fig.update_layout(
                height=380, margin=dict(l=10, r=10, t=30, b=10), title=f"{sym} Spot Price ({lookback})"
            )
            st.plotly_chart(fig, use_container_width=True)

            # Volume (base units)
            st.plotly_chart(
                px.bar(df_p, x="close_time", y="volume", title=f"{sym} Spot Volume"),
                use_container_width=True,
            )
        else:
            st.info("No spot OHLC available right now (Binance/Kraken/Bitstamp all blocked or empty).")

        # --- Derivatives (Binance market-data endpoints proxied by backend) ---
        oi = api_get("/derivs/oi_hist", {"symbol": sym, "period": deriv_period, "limit": 500}) if BACKEND else []
        ls = api_get("/derivs/ls_ratio", {"symbol": sym, "period": deriv_period, "limit": 500}) if BACKEND else []
        tk = api_get("/derivs/taker_ratio", {"symbol": sym, "period": deriv_period, "limit": 500}) if BACKEND else []

        dfo, dfl, dft = (tsify(oi), tsify(ls), tsify(tk))

        if not dfo.empty and "sumOpenInterestValue" in dfo:
            st.plotly_chart(
                px.area(dfo, x="t", y="sumOpenInterestValue", title=f"{sym} OI Notional (USD)"),
                use_container_width=True,
            )
        else:
            st.info("No OI hist (Binance blocked or empty).")

        if not dfl.empty and {"longAccount", "shortAccount"}.issubset(dfl.columns):
            st.plotly_chart(
                px.line(dfl, x="t", y=["longAccount", "shortAccount"], title=f"{sym} Global Long/Short Account Ratio"),
                use_container_width=True,
            )

        if not dft.empty and {"buyVol", "sellVol"}.issubset(dft.columns):
            st.plotly_chart(
                px.line(dft, x="t", y=["buyVol", "sellVol"], title=f"{sym} Taker Buy/Sell Volume"),
                use_container_width=True,
            )

    st.divider()
    st.subheader("Aggregated OI (Binance + Bybit + OKX)")
    for sym in symbols:
        js = api_get("/agg/oi", {"symbol": sym}) if BACKEND else {}
        rows = js.get("exchanges", []) if isinstance(js, dict) else []
        if rows:
            st.dataframe(pd.DataFrame(rows))
        series = api_get("/agg/oi_series", {"symbol": sym, "bucket": "daily", "days": 60}) if BACKEND else {}
        srf = pd.DataFrame(series.get("series", []))
        if not srf.empty:
            srf["t"] = pd.to_datetime(srf["t"], unit="ms", utc=True)
            st.plotly_chart(
                px.line(srf, x="t", y="oi_usd", title=f"{sym} Aggregated OI Notional (USD)"),
                use_container_width=True
            )
        elif not rows:
            if js.get("_error") == "no_backend":
                st.info("Backend not configured. Set BACKEND_URL in Secrets.")
            else:
                st.info("No aggregated OI data yet.")

# ---- Liquidations ----
with t_liq:
    st.subheader("Live Liquidations Heatmap (Backend buffer from Binance !forceOrder@arr)")
    sym = st.selectbox("Symbol", symbols, index=0, key="liq_sym")

    if not BACKEND:
        st.info("Backend not configured. Set BACKEND_URL in Secrets.")
    else:
        # Pull heatmap using the current slider value (previously used session_state by mistake)
        hm = api_get("/liq/heatmap", {"symbol": sym, "minutes": liq_minutes, "bins": bins})

        # Optional: show basic buffer status if available (won't break if endpoint missing)
        status = api_get("/liq/status", {"minutes": liq_minutes})
        if isinstance(status, dict):
            buf_n = status.get("count") or status.get("n") or status.get("events")
            if buf_n is not None:
                st.caption(f"Backend liq buffer (last {liq_minutes}m): {buf_n} events")

        if not isinstance(hm, dict) or not hm.get("x") or not hm.get("y") or not hm.get("z"):
            # If backend just started, it may take ~30–60s to accumulate prints.
            err = hm.get("_error") if isinstance(hm, dict) else None
            if err == "no_backend":
                st.info("Backend not configured. Set BACKEND_URL in Secrets.")
            else:
                st.info("Waiting for liquidation prints (or backend not running yet)...")
        else:
            # y comes in ms
            ydt = pd.to_datetime(hm["y"], unit="ms", utc=True)
            fig = go.Figure(data=go.Heatmap(z=hm["z"], x=hm["x"], y=ydt))
            fig.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10), title=f"{sym} – Liq Notional Heatmap")
            st.plotly_chart(fig, use_container_width=True)

# ---- Overview ----
with t_over:
    st.write(
        "This page uses only free/public APIs and your backend to avoid Cloud WAF blocks. "
        "Use the sidebar to switch symbols and the tabs to dive in."
    )
