# backend/routers/derivs.py
# ======================
from __future__ import annotations
from fastapi import APIRouter
from services.providers import binance_oi_hist, binance_ls_ratio, binance_taker_ratio

router = APIRouter(prefix="/derivs", tags=["derivs"])

@router.get("/oi_hist")
def oi_hist(symbol: str = "BTCUSDT", period: str = "1h", limit: int = 500):
    return binance_oi_hist(symbol, period, limit)

@router.get("/ls_ratio")
def ls_ratio(symbol: str = "BTCUSDT", period: str = "1h", limit: int = 500):
    return binance_ls_ratio(symbol, period, limit)

@router.get("/taker_ratio")
def taker_ratio(symbol: str = "BTCUSDT", period: str = "1h", limit: int = 500):
    return binance_taker_ratio(symbol, period, limit)