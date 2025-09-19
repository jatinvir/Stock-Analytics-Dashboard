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

@app.get("/ping_db")
def ping_db():
    try:
        with _db_connect() as conn, conn.cursor() as cur:
            cur.execute("select 1;")
            return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/ingest_one/{symbol}")
def ingest_one(symbol: str):
    try:
        symbol = symbol.strip().upper()
        today = date.today()
        # last 20 days of data
        start_date = today - timedelta(days=20)
        with _db_connect() as conn:
            print("Connected to the database successfully.")
            with conn.cursor() as cur:
                # check if the symbol actually exists
                cur.execute("SELECT 1 FROM symbols WHERE symbol = %s;", (symbol,))
                if cur.fetchone() is None:
                    return {"status": "error", "details": f"Symbol {symbol} not found in database."}
                
                # fetch data from yfinance
                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=start_date, end=today, interval="1d")
                if hist.empty:
                    return {"status": "error", "details": f"No historical data found for {symbol} from {start_date} to {today}."}
                rows = hist.shape[0]

                # insert data into prices table
                for index, row in hist.iterrows():
                    print(f"Inserting data for {symbol} on {index.date()}")
                    print((f"{symbol}", index.date(), row['Open'], row['High'], row['Low'], row['Close'], int(row['Volume'])))
                    cur.execute("""
                        INSERT INTO prices (symbol, date, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, date) DO UPDATE
                        SET open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume;
                        
                                """, (symbol, index.date(), row['Open'], row['High'], row['Low'], row['Close'], int(row['Volume']))
                        )
                return {"status": "ok", "symbol": symbol, "rows": rows, "window_days": (today - start_date).days}
                
    except Exception as e:
        return {"status": "error", "details": f"Error fetching data for {symbol}: {str(e)}"}


@app.post("/ingest_all/")
def ingest_all():
    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol FROM symbols;")
                symbols = [row[0] for row in cur.fetchall()]
                if not symbols:
                    return {"status": "error", "details": "No symbols found in database."}
        results = []
        processed = 0
        succeeded = 0
        failed = 0
        for symbol in symbols:
            res = ingest_one(symbol)
            processed += 1
            if res.get("status") == "error":
                results.append({"symbol": symbol, "status": "error", "details": res.get("details")})
                failed += 1
                continue
            succeeded += 1
            results.append(res)
        summary = []
        for result in results:
            symbol = result.get("symbol")
            rows_added = result.get("rows", 0)
            window = result.get("window_days", 0)
            summary.append(f"{symbol}: {rows_added} rows")
        return {"status": "ok","processed": processed, "succeeded": succeeded, "failed": failed, "window_days": window,
                 "summary": summary}
    except Exception as e:
        return {"status": "error", "details": str(e)}