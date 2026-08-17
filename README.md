# LinkPlease Assignment

FastAPI implementation for the LinkPlease tech intern assignment.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PSEUDOGRAM_API_KEY="your-key"
uvicorn app:app --host 0.0.0.0 --port 8000
```

On Windows PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:PSEUDOGRAM_API_KEY="your-key"
uvicorn app:app --host 0.0.0.0 --port 8000
```

## Environment

- `PSEUDOGRAM_API_KEY`: required to call the mock Instagram API.
- `PSEUDOGRAM_BASE_URL`: defaults to `https://pseudogram-api.onrender.com`.
- `DATABASE_URL`: defaults to `linkplease.db`.
- `VERIFY_SIGNATURES`: set to `true` to reject invalid webhook signatures.

## API

- `POST /rules`
- `POST /webhook`
- `GET /stats`

The service persists rules, events, and delivery jobs in SQLite. Webhooks write the work to disk before returning. A background worker sends DMs, backs off on transient failures, respects the 10-per-minute send limit, polls accepted DMs until terminal status, and retries DMs that later fail.
