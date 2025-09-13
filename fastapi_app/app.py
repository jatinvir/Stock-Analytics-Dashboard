from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
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
def symbols(q: str = Query(None, min_length=1), limit: int = Query(0, ge=0), offset: int = Query(0, ge=0)):
    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                if q is None or limit is None or offset is None:
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

