# backend/app.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import macro, agg, liq, derivs  # these files live in backend/routers/*.py

app = FastAPI(title="Crypto Macro Suite")

# CORS: Streamlit may call backend server-to-server; "*" is easiest while you test
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

@app.get("/health")
def health():
    return {"ok": True}

# Routers (you already have these)
app.include_router(macro.router)
app.include_router(agg.router)
app.include_router(liq.router)
app.include_router(derivs.router)
