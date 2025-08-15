# backend/app.py
# ===============
from __future__ import annotations
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import macro, derivs, agg, liq
from services.storage import init_db

app = FastAPI(title="Crypto Macro Backend (Free APIs)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ensure db exists
init_db()

app.include_router(macro.router)
app.include_router(derivs.router)
app.include_router(agg.router)
app.include_router(liq.router)

@app.get("/health")
def health():
    return {"ok": True}