from fastapi import FastAPI, HTTPException, Query
from datetime import date, timedelta
from typing import Optional
from pandas import DataFrame
import pandas as pd
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


def calculate_moving_average(symbol: str, window: int, date_from: Optional[date], date_to: Optional[date]):
    symbol = symbol.strip().upper()
    with _db_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM symbols where symbol = %s", (symbol,))
        
        if cur.fetchone() is None:
            return {"status": "error", "details": f"Symbol {symbol} not found in database."}

        #retrieve the date and the closing price
        cur.execute(
            """
            SELECT date, close
            FROM prices
            WHERE symbol = %s
                AND date >= COALESCE(%s, date)
                AND date <= COALESCE(%s, date)
            ORDER BY date ASC
            """,
            (symbol, date_from, date_to),
        )

        date_prices_data = cur.fetchall()

        if not date_prices_data:
            return {"status": "error", "details": f"No price data available for {symbol} in that range"}
        
        df_avg = DataFrame(date_prices_data, columns=["date", "close"])

        df_avg["MA"] = df_avg["close"].rolling(window=window).mean()

        return {"status": "ok", "rows": df_avg.to_dict(orient="records")}


