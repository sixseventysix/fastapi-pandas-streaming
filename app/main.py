from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from typing import Optional
import pandas as pd
import time

app = FastAPI(title="FastAPI + pandas NDJSON streaming")


def _ndjson_generator(path: str, chunksize: int, cols: Optional[str], query: Optional[str]):
    usecols = cols.split(",") if cols else None

    for chunk in pd.read_csv(path, chunksize=chunksize, usecols=usecols):
        if query:
            chunk = chunk.query(query, engine="python")
        ndjson = chunk.to_json(orient="records", lines=True)
        if not ndjson.endswith("\n"):
            ndjson += "\n"
        yield ndjson

        # uncomment this to see the "streaming" nature of the response. before sending the next chunk we invoke a sleep of 1s
        # time.sleep(1)

@app.get("/stream/rows", summary="Stream CSV rows as NDJSON (JSON Lines)")
def stream_rows(
    path: str,
    chunksize: int = 50,
    cols: Optional[str] = None,
    query: Optional[str] = None,
):
    generator = _ndjson_generator(path=path, chunksize=chunksize, cols=cols, query=query)
    return StreamingResponse(generator, media_type="application/x-ndjson")


@app.get("/")
def root():
    return {
        "try": "/stream/rows?path=data/sample.csv&chunksize=10000",
        "format": "application/x-ndjson",
        "notes": "Params: path (CSV), chunksize, cols (csv), query (pandas query)"
    }
