# backend/app.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import macro, agg, liq, derivs

# ✅ import AFTER standard libs to avoid circulars
from services.ws_liq import RUN_WS, start_liq_buffer

app = FastAPI(title="Crypto Macro Suite")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

@app.get("/health")
def health():
    return {"ok": True}

# Routers
app.include_router(macro.router)
app.include_router(agg.router)
app.include_router(liq.router)
app.include_router(derivs.router)

# 🔌 start the WS buffer only when RUN_WS=1
@app.on_event("startup")
def kick_ws():
    if RUN_WS:
        start_liq_buffer()
