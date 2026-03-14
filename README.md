# Simple Polymarket Parquet Explorer

This repo now includes:

- `api/` FastAPI backend for parquet browsing (`local` or `s3` source)
- `frontend/` Angular UI for explorer + market detail chart/table

## Backend setup

```powershell
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

Run API:

```powershell
.\venv\Scripts\python.exe -m uvicorn api.main:app --reload --port 8000
```

### Backend config

Environment variables:

- `DATA_SOURCE=LOCAL|S3` (case-insensitive, default: `local`)
- `LOCAL_DATA_ROOT=./market_data`
- `DATA_CACHE_TTL_SECONDS=15`
- `INCLUDE_PART_FILES=false` (default: hidden)
- `AWS_REGION=us-east-1` (or `AWS_DEFAULT_REGION`)
- `S3_BUCKET=...` (or `S3_BUCKET_NAME`, required when `DATA_SOURCE=s3`)
- `S3_PREFIX=optional/path`

## Frontend setup

```powershell
cd frontend
npm install
npm start
```

Angular dev server runs on `http://localhost:4200` and proxies `/api/*` to `http://localhost:8000`.

## API endpoints

- `GET /health`
- `GET /api/v1/events`
- `GET /api/v1/markets?event_slug=&q=&limit=&offset=`
- `GET /api/v1/markets/{market_id}/metadata`
- `GET /api/v1/markets/{market_id}/rows?limit=&offset=`
- `GET /api/v1/markets/{market_id}/series?max_points=`
- `GET /api/v1/markets/{market_id}/stats`

## Tests

```powershell
.\venv\Scripts\python.exe -m pytest -q
```

## Switching from local to S3

1. Keep frontend unchanged.
2. Set backend env vars:

```powershell
$env:DATA_SOURCE='s3'
$env:S3_BUCKET='your-bucket' # or set S3_BUCKET_NAME
$env:S3_PREFIX='optional/prefix'
$env:AWS_REGION='us-east-1'
```

3. Restart backend.

The API contract is unchanged, so the UI continues working without code changes.

## Security note

Do not expose AWS long-lived credentials in frontend code. Use backend IAM roles/credentials for S3 access.
