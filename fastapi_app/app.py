from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import date
from typing import Optional
import psycopg
import os

app = FastAPI()

def _db_connect():
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    return psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)

class Symbol(BaseModel):
    symbol: str
    name: str  

class SymbolUpdate(BaseModel):
    name: str

@app.get("/healthz")
def healthz():
    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": "error", "details": str(e)}, status_code=500)


@app.get("/symbols")
def symbols(q: str = Query(None, min_length=1), limit: int = Query(50, ge=0), offset: int = Query(0, ge=0)):
    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                if q is None:
                    cur.execute("""
                        SELECT symbol, name 
                        FROM symbols 
                        ORDER BY symbol
                        LIMIT %s OFFSET %s;
                         """, (limit, offset)
                    )
                else:
                    cur.execute("""
                        SELECT symbol, name 
                        FROM symbols 
                        WHERE symbol ILIKE %s OR name ILIKE %s
                        ORDER BY symbol
                        LIMIT %s OFFSET %s;
                        """, (f"%{q}%", f"%{q}%", limit, offset)
                    )
                rows = cur.fetchall()
            results = [{"symbol": r[0], "name": r[1]} for r in rows]
        return JSONResponse({"status": "ok", "data": results})
    except Exception as e:
        return JSONResponse({"status": "error", "details": str(e)}, status_code=500)

@app.get("/symbols/{symbol}")
def get_symbol(symbol: str):
    symbol = symbol.strip().upper()
    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM symbols WHERE symbol = %s;", (symbol,))
                row = cur.fetchone()
                if row is None:
                    raise HTTPException(status_code=404, detail="Symbol not found")
                result = {"symbol": row[0], "name": row[1]}
                return JSONResponse({"status": "ok", "data": result})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/symbols", status_code=201)
def create_symbol(payload: Symbol):
    symbol = payload.symbol.strip().upper()
    name = payload.name.strip()

    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO symbols (symbol, name)
                    VALUES (%s, %s)
                    ON CONFLICT (symbol) DO NOTHING;
                    """,
                    (symbol, name)
                )
                if cur.rowcount == 0:
                    raise HTTPException(status_code=409, detail="Symbol already exists")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/symbols/{symbol}", status_code=204)
def delete_symbol(symbol: str):
    symbol = symbol.strip().upper()
    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM symbols WHERE symbol = %s;
                    """,
                    (symbol,)
                )
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Symbol not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/symbols/{symbol}", status_code=200)
def update_symbol(symbol: str, payload: SymbolUpdate):
    symbol = symbol.strip().upper()
    name = payload.name.strip()

    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE symbols SET name = %s WHERE symbol = %s;
                    """
                    , (name, symbol)
                )
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Symbol not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


## prices
@app.get("/prices/")
def get_prices(limit: int = Query(30, ge=1, le=200), offset: int = Query(0, ge=0), symbol: str = Query(..., min_length=1), date_from: Optional[date] = Query(None), date_to: Optional[date] = Query(None)):
    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                sym = symbol.strip().upper()
                if date_from and date_to and date_from > date_to:
                    raise HTTPException(status_code=400, detail="date_from cannot be greater than date_to")
                where = ["p.symbol = %s"]
                params = [sym]
                if date_from:
                    where.append("p.date >= %s")
                    params.append(date_from)
                if date_to:
                    where.append("p.date <= %s")
                    params.append(date_to)
                where_clause = " AND ".join(where)
                lim_plus_one = limit + 1
                cur.execute(
                    f"""
                    SELECT p.date, p.open, p.high, p.low, p.close, p.volume
                    FROM prices p
                    WHERE {where_clause}
                    ORDER BY p.date DESC
                    LIMIT %s OFFSET %s;
                    """
                    , (*params, lim_plus_one, offset )
                )
                rows = cur.fetchall()
            has_more = len(rows) > limit
            if has_more:
                rows = rows[:limit]
            results = [{"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5], "has_more": has_more} for r in rows]
        return ({"status": "ok", "data": results})
    except Exception as e:
        return JSONResponse({"status": "error", "details": str(e)}, status_code=500)

            
@app.get("/prices/{symbol}/latest")
def get_latest_price(symbol: str):
    symbol = symbol.strip().upper()
    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT date, open, high, low, close, volume
                    FROM public.prices
                    WHERE symbol = %s
                    ORDER BY date DESC
                    LIMIT 1;
                    """,
                    (symbol,)
                )
                row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="No price data found for the symbol")
            result = {"date": row[0], "open": row[1], "high": row[2], "low": row[3], "close": row[4], "volume": row[5]}
        return ({"status": "ok", "data": result})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
                          