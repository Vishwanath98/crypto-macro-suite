# backend/services/providers.py
# ======================
from __future__ import annotations
import os, time, math, requests
from typing import Dict, Any, Optional

BINANCE_FAPI = os.getenv("BINANCE_FAPI", "https://fapi.binance.com")
COINGECKO_API = os.getenv("COINGECKO_API", "https://api.coingecko.com/api/v3")
BYBIT_API = os.getenv("BYBIT_API", "https://api.bybit.com")
OKX_API = os.getenv("OKX_API", "https://www.okx.com")

UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def get_json(url: str, params: Optional[dict] = None, retries: int = 2, backoff: float = 0.6) -> Any:
    last = None
    for i in range(retries + 1):
        try:
            r = requests.get(url, params=params or {}, headers=UA, timeout=20)
            if r.status_code in (403, 418, 451, 429):
                last = {"_error": f"http_{r.status_code}", "_url": url}
            else:
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last = {"_error": str(e), "_url": url}
        time.sleep(backoff * (2 ** i))
    return last

# -------- CoinGecko (free) --------
# Current global totals (no key):
#   GET /global → { data: { total_market_cap: {usd: ...}, total_volume: {usd: ...}, market_cap_percentage: {btc: ...} } }

def cg_global() -> Dict[str, Any]:
    js = get_json(f"{COINGECKO_API}/global")
    return (js or {}).get("data", {})

# Current BTC/ETH market cap

def cg_btc_eth_caps() -> Dict[str, float]:
    js = get_json(f"{COINGECKO_API}/coins/markets", params={
        "vs_currency": "usd",
        "ids": "bitcoin,ethereum",
        "order": "market_cap_desc",
        "per_page": 2, "page": 1, "sparkline": "false"
    })
    caps = {"bitcoin": None, "ethereum": None}
    if isinstance(js, list):
        for row in js:
            caps[row.get("id")] = float(row.get("market_cap") or 0)
    return {"btc": caps.get("bitcoin") or 0.0, "eth": caps.get("ethereum") or 0.0}


# -------- Binance (free) --------

def binance_oi_hist(symbol: str, period: str = "1h", limit: int = 500):
    url = f"{BINANCE_FAPI}/futures/data/openInterestHist"
    return get_json(url, {"symbol": symbol, "period": period, "limit": limit}) or []

def binance_ls_ratio(symbol: str, period: str = "1h", limit: int = 500):
    url = f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio"
    return get_json(url, {"symbol": symbol, "period": period, "limit": limit}) or []

def binance_taker_ratio(symbol: str, period: str = "1h", limit: int = 500):
    url = f"{BINANCE_FAPI}/futures/data/takerlongshortRatio"
    return get_json(url, {"symbol": symbol, "period": period, "limit": limit}) or []

# Latest OI notional (USD) via hist last point (sumOpenInterestValue)

def binance_oi_usd_latest(symbol: str) -> Optional[float]:
    js = binance_oi_hist(symbol, period="5m", limit=1)
    if isinstance(js, list) and js:
        v = js[-1].get("sumOpenInterestValue")
        try: return float(v)
        except: return None
    return None


# -------- Bybit (free public) --------
# v5 GET /v5/market/open-interest (category=linear, intervalTime=5min)
# v5 GET /v5/market/tickers (category=linear)

def bybit_oi_usd_latest(symbol: str) -> Optional[float]:
    oi = get_json(f"{BYBIT_API}/v5/market/open-interest", {
        "category": "linear", "symbol": symbol, "intervalTime": "5min", "limit": 1
    })
    if not isinstance(oi, dict):
        return None
    lst = (((oi.get("result") or {}).get("list")) or [])
    try:
        open_interest = float(lst[-1].get("openInterest")) if lst else None
    except: open_interest = None
    if open_interest is None:
        return None
    # get last price to convert to USD
    tk = get_json(f"{BYBIT_API}/v5/market/tickers", {"category": "linear", "symbol": symbol})
    try:
        last_price = float(((tk.get("result") or {}).get("list") or [{}])[0].get("lastPrice"))
    except:
        last_price = None
    if last_price is None:
        return None
    # Bybit linear perps: 1 contract ~ 1 base asset; multiply by price to get USD notional (approx)
    return float(open_interest * last_price)


# -------- OKX (free public) --------
# GET /api/v5/public/open-interest?instId=BTC-USDT-SWAP → oi, oiCcy
# GET /api/v5/market/ticker?instId=BTC-USDT-SWAP → last

def okx_oi_usd_latest(symbol: str) -> Optional[float]:
    inst = symbol.replace("USDT", "-USDT-SWAP")
    oi = get_json(f"{OKX_API}/api/v5/public/open-interest", {"instId": inst})
    if not isinstance(oi, dict):
        return None
    data = (oi.get("data") or [])
    if not data:
        return None
    try:
        oi_ccy = float(data[0].get("oiCcy") or 0)
    except:
        oi_ccy = 0.0
    tk = get_json(f"{OKX_API}/api/v5/market/ticker", {"instId": inst})
    try:
        last = float(((tk.get("data") or [{}])[0]).get("last"))
    except:
        last = None
    if last is None:
        return None
    return float(oi_ccy * last)