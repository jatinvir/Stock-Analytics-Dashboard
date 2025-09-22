from fastapi import FastAPI, HTTPException, Query
from datetime import date, timedelta
import psycopg
import os
import yfinance as yf
from dotenv import load_dotenv
load_dotenv(".env.dev")

app = FastAPI()

def _db_connect():
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    print("DB Connection details:", dbname, user, host, port)
    return psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)


def ingest_prices(cur, symbol: str, start_date: date, end_date: date):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(start=start_date, end=end_date, interval="1d")
    if hist.empty:
        return {"status": "error", "details": f"No historical data found for {symbol} from {start_date} to {end_date}."}

    rows = 0
    for index, row in hist.iterrows():
        open_price = row['Open']
        high_price = row['High']
        low_price = row['Low']
        close_price = row['Close']
        volume = int(row['Volume'])

        if any(x is None for x in (open_price, high_price, low_price, close_price, volume)):
            print(f"Skipping insertion for {symbol} on {index.date()} due to missing data.")
            continue
        
        cur.execute("""
            INSERT INTO prices (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
                    """, (symbol, index.date(), open_price, high_price, low_price, close_price, volume))
        rows += 1
    return {"status": "ok", "symbol": symbol, "rows": rows, "window_days": (end_date - start_date).days}

def ingest_one_symbol(cur, symbol: str):
    today = date.today()
    start_date = today - timedelta(days=20)
    symbol = symbol.strip().upper()
    with _db_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM symbols WHERE symbol = %s", (symbol,))
        if cur.fetchone() is None:
            return {"status": "error", "details": f"Symbol {symbol} not found in database."}
        result = ingest_prices(cur, symbol, start_date, today)
    return result

def ingest_all_symbols(lookback_days: int = 20):
    today = date.today()
    start_date = today - timedelta(days=lookback_days)
    results = []
    succeeded = 0
    failed = 0
    with _db_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT symbol FROM symbols;")
        symbols = [row[0] for row in cur.fetchall()]
        if not symbols:
            return {"status": "error", "details": "No symbols found in database."}
        for symbol in symbols:
            result = ingest_prices(cur, symbol, start_date, today)
            if result.get("status") == "error":
                failed += 1
            else:
                succeeded += 1
            results.append(result)
    return {
        "status": "ok",
        "processed": len(symbols),
        "succeeded": succeeded,
        "failed": failed,
        "window_days": lookback_days,
        "summary": [f"{r.get('symbol', 'N/A')}: {r.get('rows', 0)} rows" for r in results if r.get("status") == "ok"],
    }
