from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from typing import Optional, Iterator
import pandas as pd

app = FastAPI(title="FastAPI + pandas NDJSON streaming (pipeline)")

# -------------------- STAGES --------------------

def read_csv_chunks(path: str, chunksize: int, usecols: Optional[list[str]]) -> Iterator[pd.DataFrame]:
    for chunk in pd.read_csv(path, chunksize=chunksize, usecols=usecols):
        yield chunk

def apply_query(chunk_it: Iterator[pd.DataFrame], query: Optional[str]) -> Iterator[pd.DataFrame]:
    if not query:
        yield from chunk_it
        return
    for df in chunk_it:
        yield df.query(query, engine="python")

def enrich_scale(chunk_it: Iterator[pd.DataFrame],
                 source_col: Optional[str],
                 scale_factor: Optional[float],
                 out_col: Optional[str]) -> Iterator[pd.DataFrame]:
    if not source_col or scale_factor is None or not out_col:
        yield from chunk_it
        return
    for df in chunk_it:
        if source_col in df.columns:
            df[out_col] = pd.to_numeric(df[source_col], errors="coerce") * scale_factor
        yield df

def encode_with_progress(chunk_it: Iterator[pd.DataFrame],
                         groupby: Optional[str]) -> Iterator[str]:
    running_counts: dict = {}
    for df in chunk_it:
        rows_json = df.to_json(orient="records")
        yield f'{{"type":"data","rows":{rows_json}}}\n'

        if groupby and groupby in df.columns:
            vc = df[groupby].value_counts(dropna=False).to_dict()
            for k, v in vc.items():
                running_counts[k] = running_counts.get(k, 0) + int(v)
            yield (
                f'{{"type":"progress","running_counts":'
                f'{pd.Series(running_counts).to_json()}}}\n'
            )

def pipeline_ndjson(path: str,
                    chunksize: int,
                    cols: Optional[str],
                    query: Optional[str],
                    scale_src: Optional[str],
                    scale_factor: Optional[float],
                    scale_out: Optional[str],
                    groupby: Optional[str]) -> Iterator[str]:
    """
    Yields NDJSON frames:
      - data frames:   {"type":"data","rows":[{...},{...}]}
      - progress frames (optional): {"type":"progress","running_counts": {...}}
    """
    usecols = cols.split(",") if cols else None

    # Stage 1: read
    stream = read_csv_chunks(path, chunksize, usecols)

    # Stage 2: filter/query
    stream = apply_query(stream, query)

    # Stage 3: enrich (scale a numeric column into a new column)
    stream = enrich_scale(stream, scale_src, scale_factor, scale_out)

    # Stage 4: encode + (optional) running group counts
    stream = encode_with_progress(stream, groupby)

    yield from stream

# -------------------- ENDPOINTS --------------------

@app.get("/stream/rows", summary="Stream CSV rows as NDJSON (JSON Lines)")
def stream_rows(
    path: str,
    chunksize: int = 50,
    cols: Optional[str] = None,
    query: Optional[str] = None,
):
    def gen():
        usecols = cols.split(",") if cols else None
        for chunk in pd.read_csv(path, chunksize=chunksize, usecols=usecols):
            if query:
                chunk = chunk.query(query, engine="python")
            ndjson = chunk.to_json(orient="records", lines=True)
            if not ndjson.endswith("\n"):
                ndjson += "\n"
            yield ndjson
    return StreamingResponse(gen(), media_type="application/x-ndjson")

@app.get("/stream/pipeline", summary="Stream multi-stage pipeline: data + progress frames (NDJSON)")
def stream_pipeline(
    path: str,
    chunksize: int = 5000,
    cols: Optional[str] = None,
    query: Optional[str] = None,
    scale_src: Optional[str] = None,          # e.g., "value"
    scale_factor: Optional[float] = None,     # e.g., 2.0
    scale_out: Optional[str] = None,          # e.g., "value_scaled"
    groupby: Optional[str] = None,            # e.g., "category"
):
    generator = pipeline_ndjson(
        path=path,
        chunksize=chunksize,
        cols=cols,
        query=query,
        scale_src=scale_src,
        scale_factor=scale_factor,
        scale_out=scale_out,
        groupby=groupby,
    )
    return StreamingResponse(generator, media_type="application/x-ndjson")

@app.get("/")
def root():
    return {
        "try_rows": "/stream/rows?path=data/sample.csv&chunksize=2",
        "try_pipeline_data_only": "/stream/pipeline?path=data/sample.csv&chunksize=2",
        "try_pipeline_enrich": "/stream/pipeline?path=data/sample.csv&chunksize=2&scale_src=value&scale_factor=2&scale_out=value_scaled",
        "try_pipeline_progress": "/stream/pipeline?path=data/sample.csv&chunksize=2&groupby=category",
        "format": "application/x-ndjson",
        "notes": "Pipeline emits NDJSON frames with type=data|progress"
    }