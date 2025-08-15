# how to run
## server
`uv run uvicorn app.main:app --reload --port 8087`
## requests
### Group by category
`curl -N "http://localhost:8087/stream/pipeline?path=data/sample.csv&chunksize=2&groupby=category"`
### Scale values of a column in pandas dataframe
`curl -N "http://localhost:8087/stream/pipeline?path=data/sample.csv&chunksize=2&scale_src=value&scale_factor=2&scale_out=value_scaled"`
### Simple row stream
`curl -N "http://localhost:8087/stream/rows?path=data/sample.csv&chunksize=2"`