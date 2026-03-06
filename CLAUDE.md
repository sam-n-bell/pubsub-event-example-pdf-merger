# event-driven-pdf-pipeline — agent reference

CDC-driven PDF pipeline. Pub/Sub events → taskiq worker fetches PDFs from
PostgreSQL, merges with pypdf, uploads to GCS. FastAPI streams the result back.

## Commands

```bash
make infra          # docker compose up -d --wait (blocks until healthy)
make init-infra     # infra + create Pub/Sub topic, Pub/Sub subscription, GCS bucket, DB table
                    # must run before subscriber/worker; re-run after volumes are wiped
make load-pdfs      # upsert pdfs/*.pdf into PostgreSQL (idempotent)
make api            # uvicorn on :8000
make worker         # taskiq worker
make subscriber     # Pub/Sub pull loop
make publish        # fire a test CDC event (defaults: documents/INSERT/ACC-001/contract)
make format         # ruff format
make lint           # ruff check
make typecheck      # ty check event_driven_pdf_pipeline/
```

## Package layout

```
event_driven_pdf_pipeline/
├── config.py           pydantic-settings; syncs PUBSUB_EMULATOR_HOST to os.environ on import
├── log.py              configure_logging() + get_logger(); must be called at each process entry point
├── broker.py           taskiq ListQueueBroker + RedisAsyncResultBackend
├── api.py              FastAPI app + lifespan
├── db/
│   ├── models.py       Base, PdfDocument (bytea column)
│   └── ops.py          engine, AsyncSessionLocal, get_session, create_tables,
│                       fetch_all_pdfs, upsert_pdf
├── storage/
│   ├── gcs.py          get_storage_client, upload_bytes, blob_exists, stream_blob
│   └── pdf.py          collect_pdfs, merge_pdfs, merge_pdfs_from_bytes
└── messaging/
    ├── tasks.py        merge_and_upload_pdfs, handle_record_deletion  (taskiq tasks)
    ├── subscriber.py   Pub/Sub streaming pull → dispatches tasks
    └── publisher.py    Typer CLI for manual event publishing
```

## Key patterns

**Logging** — call `configure_logging()` once at each process entry point:
- `api.py` lifespan
- `messaging/subscriber.py` `run()`
- `messaging/tasks.py` module level (worker picks it up on import)
- scripts call it at the top

**DB sessions** — two patterns depending on context:
- FastAPI: `session: AsyncSession = Depends(get_session)` (from `db.ops`)
- Tasks / scripts: `async with AsyncSessionLocal() as session:`

**GCS streaming** — `stream_blob()` is a sync generator (GCS client is sync-only).
Wrap with `iterate_in_threadpool` in async contexts (see `api.py`).

**Emulator wiring**
- Pub/Sub: `config.py` syncs `pubsub_emulator_host` → `os.environ["PUBSUB_EMULATOR_HOST"]` at import time; Google client reads it natively
- GCS: `get_storage_client()` checks `settings.storage_emulator_host` and passes `AnonymousCredentials` + `api_endpoint`

## CDC event shape

GoldenGate JSON operation-based formatter. `op_type` uses single chars:
`I` (INSERT), `U` (UPDATE), `D` (DELETE).

```json
{
  "table": "SCHEMA.DOCUMENTS",
  "op_type": "I",
  "op_ts": "2024-01-15 10:23:45.000000",
  "current_ts": "2024-01-15 10:23:45.123000",
  "pos": "00000000000000001234",
  "primary_keys": ["id"],
  "before": null,
  "after": { "id": "rec-001", "account_id": "ACC-001", "document_type": "contract" }
}
```

- INSERT → `before=null`, `after=row`
- UPDATE → `before=row`, `after=row`
- DELETE → `before=row`, `after=null`

Tables that trigger `merge_and_upload_pdfs` (reads from `after`):
`documents`, `attachments`, `files`.
Any table + `D` triggers `handle_record_deletion` (reads `id` from `before`).

GCS blob path: `{account_id}/{document_type}.pdf`
API endpoint: `GET /documents/{account_id}/{document_type}`

Inspect bucket contents directly (no UI):
```bash
curl http://localhost:4443/storage/v1/b/pdf-pipeline-bucket/o
```

## Environment

All config via `.env` (copy from `.env.example`). Key vars:
- `PUBSUB_EMULATOR_HOST` — set by config.py automatically from `pubsub_emulator_host`
- `STORAGE_EMULATOR_HOST` — e.g. `http://localhost:4443`
- `DATABASE_URL` — must use `postgresql+asyncpg://` scheme
- `REDIS_URL`
- `JSON_LOGS=true` — switch structlog to JSON renderer (default: pretty console)
