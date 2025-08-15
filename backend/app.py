from fastapi import FastAPI
from routers import health, macro, agg, liq
from services.ws_liq import RUN_WS, start_liq_buffer
import threading

app = FastAPI(title="Crypto Macro Suite Backend")

app.include_router(health.router, prefix="")
app.include_router(macro.router,  prefix="/macro")
app.include_router(agg.router,    prefix="/agg")
app.include_router(liq.router,    prefix="/liq")

@app.on_event("startup")
def _startup():
    if RUN_WS:  # env RUN_WS defaults to "1" in services/ws_liq.py
        t = threading.Thread(target=start_liq_buffer, daemon=True)
        t.start()
