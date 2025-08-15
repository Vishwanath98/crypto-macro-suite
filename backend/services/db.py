import os, sqlite3, threading, time

_DB_PATH = os.getenv("DB_PATH", "data/app.db")
os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
_lock = threading.Lock()

def _conn():
    con = sqlite3.connect(_DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    return con

def init_db():
    con = _conn()
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS liq_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        time_ms INTEGER NOT NULL,
        price REAL NOT NULL,
        side TEXT CHECK(side IN ('BUY','SELL')) NOT NULL,
        qty REAL, quote_qty REAL
    )""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_liq_time ON liq_events(symbol, time_ms)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS macro_snapshots (
        time_ms INTEGER PRIMARY KEY,
        total REAL, volume REAL,
        btc REAL, eth REAL, alt REAL,
        btc_dom REAL, fear_greed INTEGER
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS agg_oi_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        time_ms INTEGER NOT NULL,
        exchange TEXT NOT NULL,
        oi_contracts REAL,
        oi_usd REAL
    )""")
    con.commit(); con.close()

init_db()
