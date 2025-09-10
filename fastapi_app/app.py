from fastapi import FastAPI
from fastapi.responses import JSONResponse
import psycopg
import os

app = FastAPI()

@app.get("/healthz")
def healthz():
    try:
        dbname = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")

        with psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": "error", "details": str(e)}, status_code=500)


@app.get("/symbols")
def symbols():
    try:
        # connect to pg inside docker
        dbname = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")

        with psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM symbols;")
                rows = cur.fetchall()
            results = [{"symbol": r[0], "name": r[1]} for r in rows]
        return JSONResponse({"status": "ok", "data": results})
    except Exception as e:
        return JSONResponse({"status": "error", "details": str(e)}, status_code=500)
