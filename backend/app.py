from fastapi import FastAPI
from routers import health, macro, agg, liq
from services.ws_liq import RUN_WS, start_liq_buffer
import threading

app = FastAPI()
app.include_router(health.router, prefix="")
app.include_router(macro.router,  prefix="/macro")
app.include_router(agg.router,    prefix="/agg")
app.include_router(liq.router,    prefix="/liq")

@app.on_event("startup")
def kick_ws():
    if RUN_WS:
        # single background worker per backend process
        t = threading.Thread(target=start_liq_buffer, daemon=True)
        t.start()
