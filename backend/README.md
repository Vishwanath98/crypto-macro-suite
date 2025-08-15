# OI Aggregator Backend (FastAPI)

## Local dev
```bash
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

## Deploy to Render (no local git required)
1. Create a GitHub repo (web UI) and add `backend/` contents.
2. On Render: **New → Web Service** → Connect your repo.
3. Settings:
   - **Root Directory**: `backend/`
   - **Runtime**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn app:app --host 0.0.0.0 --port $PORT`
4. Environment variables (optional but recommended):
   - `SYMBOLS=BTCUSDT,ETHUSDT`
   - `ENABLE_BYBIT=0`
   - `ENABLE_OKX=0`
   - `DB_PATH=/data/data.db` (if you attach a Render Disk for persistence)
5. (Optional) **Persistence**: Add a Render Disk (e.g., 1GB) and mount to `/data`.
6. Test:
   - `GET /health`
   - `GET /oi/BTCUSDT`
   - `POST /snapshot`

### Automated snapshots (choose one)
- **Render Scheduled Job**: New → Cron Job → command:
  `curl -fsSL https://<your-backend-onrender.com>/snapshot`
  (every 5–10 minutes)
- **GitHub Actions**: set repo secret `BACKEND_URL` and use the provided workflow in `.github/workflows/snapshot.yml`.
