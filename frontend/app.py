from __future__ import annotations
import os, time
from datetime import datetime, timedelta, timezone
from typing import Optional, Any, Dict, List

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import requests
import streamlit as st

st.set_page_config(page_title="Crypto Macro (Free)", page_icon="📊", layout="wide")
UTC = timezone.utc

# Backend base URL (set this in Streamlit Secrets or env)
BACKEND = st.secrets.get("BACKEND_URL", os.environ.get("BACKEND_URL", ""))
if not BACKEND:
    st.warning("Set BACKEND_URL in Streamlit Secrets (or env) to enable backend-powered charts.")

# --------------------------
# HTTP helpers
# --------------------------
@st.cache_data(show_spinner=False)
def jget(url: str, params: Optional[dict] = None, retries: int = 2):
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

def _to_list(payload: Any) -> List[Dict[str, Any]]:
    """
    Normalize a backend response to a list-of-dicts for pandas.
    Returns [] on any error-ish payload.
    """
    # Already a list?
    if isinstance(payload, list):
        # must be list of dicts (Binance style); otherwise bail
        return payload if (not payload or isinstance(payload[0], dict)) else []
    # Dict error shapes: our backend uses {"_error": "..."}; Binance uses {"code": -X, "msg":"..."}
    if isinstance(payload, dict):
        if payload.get("_error") is not None:
            return []
        if "code" in payload and payload.get("code") not in (None, 0, "0"):
            return []
        # Some APIs return {"data":[...]} or {"rows":[...]}
        for k in ("data", "rows", "result", "list", "series"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
        # Otherwise it's a single object — wrap it if it has multiple fields
        return [payload] if payload else []
    # Anything else -> []
    return []

def bget(path: str, params: Optional[dict] = None) -> List[Dict[str, Any]]:
    """
    Call the backend and always return a list for DataFrame creation.
    If the backend returned an error dict, show a tiny warning and return [].
    """
    if not BACKEND:
        return []
    js = jget(f"{BACKEND}{path}", params or {})
    if isinstance(js, dict) and js.get("_error"):
        st.warning(f"Backend error at {path}: {js.get('_error')}")
        return []
    return _to_list(js)

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

    try:
        js = jget(
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

    try:
        js = jget("https://api.kraken.com/0/public/OHLC", {"pair": kr_pair, "interval": g // 60})
        data = (js.get("result") or {}).get(kr_pair, [])
        if isinstance(data, list) and data:
            df = pd.DataFrame(data, columns=["t","open","high","low","close","vwap","volume","count"])
            for c in ["open","high","low","close","volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["open_time"] = pd.to_datetime(df["t"], unit="s", utc=True)
            df["close_time"] = df["open_time"] + pd.to_timedelta(g, unit="s")
            return df[["open_time","open","high","low","close","volume","close_time"]]
    except Exception:
        pass

    try:
        js = jget(f"https://www.bitstamp.net/api/v2/ohlc/{bs_pair}/", {"step": g, "limit": 1000})
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

t_over, t_macro, t_derivs, t_liq = st.tabs(["Overview", "Macro", "Derivatives", "Liquidations"])

# ---- Macro ----
with t_macro:
    st.subheader("Market Caps & BTC Dominance (from snapshots)")
    rng = st.select_slider("Range (days)", [30, 90, 180, 365], value=180)
    js = jget(f"{BACKEND}/macro/series", {"bucket": "daily", "days": rng}) if BACKEND else {"series": []}
    df = pd.DataFrame(js.get("series", []))
    if df.empty:
        st.info("No macro snapshots yet. On Render, run /macro/snapshot via cron or the provided GitHub Action.")
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

# ---- Derivatives ----
def tsify(rows: Any) -> pd.DataFrame:
    """Safe DataFrame builder that tolerates error payloads and varying time keys."""
    rows = _to_list(rows)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for k in ("timestamp", "time", "T"):
        if k in df.columns:
            # Tolerate strings, ints, etc.
            try:
                df["t"] = pd.to_datetime(pd.to_numeric(df[k], errors="coerce"), unit="ms", utc=True)
            except Exception:
                try:
                    df["t"] = pd.to_datetime(df[k], unit="ms", utc=True)
                except Exception:
                    pass
            break
    return df

with t_derivs:
    st.subheader("Spot Price, OI, Long/Short, Taker Flow (Free Public Endpoints via Backend)")
    for sym in symbols:
        st.markdown(f"### {sym}")
        lookback = st.selectbox(
            f"Price lookback for {sym}", ["7D", "30D"], index=0, key=f"lb_{sym}"
        )
        delta = {"7D": timedelta(days=7), "30D": timedelta(days=30)}[lookback]
        now = datetime.now(UTC)
        start_ms = int((now - delta).timestamp() * 1000)
        end_ms = int(now.timestamp() * 1000)

        df_p = spot_ohlc(sym, deriv_period, start_ms, end_ms)
        if not df_p.empty:
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

            st.plotly_chart(
                px.bar(df_p, x="close_time", y="volume", title=f"{sym} Spot Volume"),
                use_container_width=True,
            )
        else:
            st.info("No spot OHLC available right now (Binance/Kraken/Bitstamp all blocked or empty).")

        # --- Derivatives via backend (robust) ---
        oi = bget("/derivs/oi_hist", {"symbol": sym, "period": deriv_period, "limit": 500})
        ls = bget("/derivs/ls_ratio", {"symbol": sym, "period": deriv_period, "limit": 500})
        tk = bget("/derivs/taker_ratio", {"symbol": sym, "period": deriv_period, "limit": 500})

        dfo, dfl, dft = (tsify(oi), tsify(ls), tsify(tk))

        if not dfo.empty and "sumOpenInterestValue" in dfo.columns:
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
        js = jget(f"{BACKEND}/agg/oi", {"symbol": sym}) if BACKEND else {}
        rows = (js or {}).get("exchanges", [])
        if rows:
            st.dataframe(pd.DataFrame(rows))
        series = jget(f"{BACKEND}/agg/oi_series", {"symbol": sym, "bucket": "daily", "days": 60}) if BACKEND else {}
        srf = pd.DataFrame((series or {}).get("series", []))
        if not srf.empty:
            srf["t"] = pd.to_datetime(srf["t"], unit="ms", utc=True)
            st.plotly_chart(px.line(srf, x="t", y="oi_usd", title=f"{sym} Aggregated OI Notional (USD)"), use_container_width=True)

# ---- Liquidations ----
with t_liq:
    st.subheader("Live Liquidations Heatmap (Backend buffer from Binance !forceOrder@arr)")
    sym = st.selectbox("Symbol", symbols, index=0)
    js = jget(
        f"{BACKEND}/liq/heatmap", {"symbol": sym, "minutes": liq_minutes, "bins": bins}
    ) if BACKEND else {"x": [], "y": [], "z": []}

    if not BACKEND:
        st.info("Backend not configured. Set BACKEND_URL in Secrets.")
    elif not js.get("x"):
        st.info("Waiting for liquidation prints (or backend not running yet)...")
    else:
        ydt = pd.to_datetime(js["y"], unit="ms", utc=True)
        fig = go.Figure(data=go.Heatmap(z=js["z"], x=js["x"], y=ydt))
        fig.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10), title=f"{sym} – Liq Notional Heatmap")
        st.plotly_chart(fig, use_container_width=True)

# ---- Overview ----
with t_over:
    st.write(
        "This page uses only free/public APIs and your backend to avoid Cloud WAF blocks. "
        "Use the sidebar to switch symbols and the tabs to dive in."
    )
