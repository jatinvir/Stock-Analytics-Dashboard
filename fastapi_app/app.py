from fastapi import FastAPI, HTTPException
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
def symbols():
    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM symbols;")
                rows = cur.fetchall()
            results = [{"symbol": r[0], "name": r[1]} for r in rows]
        return JSONResponse({"status": "ok", "data": results})
    except Exception as e:
        return JSONResponse({"status": "error", "details": str(e)}, status_code=500)

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
            