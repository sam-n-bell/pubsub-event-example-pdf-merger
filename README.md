# cdc-pdf-pipeline

Demo of a CDC-driven PDF processing pipeline. Mocked infrastructure runs in
Docker; the Python processes run on the host via `uv`.

## Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │               Docker Compose                  │
   publisher CLI    │  ┌────────────────┐  ┌──────────────────────┐ │
   (manual test) ──▶│  │ Pub/Sub emu    │  │   fake-gcs-server    │ │
                    │  │   :8085        │  │     :4443 (HTTP)     │ │
                    │  └──────┬─────────┘  └──────────▲───────────┘ │
                    │         │                       │             │
                    │  ┌──────▼─────────┐  ┌──────────────────────┐ │
                    │  │    Redis       │  │     PostgreSQL        │ │
                    │  │    :6379       │  │       :5432           │ │
                    │  └────────────────┘  └──────────▲───────────┘ │
                    └──────────────────────────────────────────────┘
                           │  (taskiq queue)           │ (PDF rows / bytea)
                    ┌──────▼──────┐            ┌───────┴──────┐
                    │ subscriber  │──── kiq ──▶│    worker    │
                    │  (host)     │            │    (host)    │
                    └─────────────┘            └──────┬───────┘
                                                      │ GCS upload
                    ┌─────────────┐            ┌──────▼───────┐
                    │   FastAPI   │─ GET ──────▶ fake-gcs      │
                    │   :8000     │            │  (streamed)   │
                    └─────────────┘            └──────────────┘
```

**Data flow:** A CDC event arrives via Pub/Sub → the subscriber enqueues a
taskiq task → the worker fetches all PDF rows from PostgreSQL, merges them
with pypdf, and uploads the result to GCS → the FastAPI server streams it
back on demand.

**CDC event → task routing:**

| table       | operation       | task                     |
|-------------|-----------------|---------------------------|
| documents   | INSERT / UPDATE | `merge_and_upload_pdfs`  |
| attachments | INSERT / UPDATE | `merge_and_upload_pdfs`  |
| files       | INSERT / UPDATE | `merge_and_upload_pdfs`  |
| *any*       | DELETE          | `handle_record_deletion` |

**GCS path convention:** `{account_id}/{document_type}.pdf`

## Prerequisites

- Docker + Docker Compose
- [uv](https://docs.astral.sh/uv/) (`brew install uv`)
- Python 3.13 (uv will fetch it automatically)

## Setup

### 1. Clone / enter the project

```bash
cd ~/Code/cdc-pdf-pipeline
```

### 2. Copy and review the env file

```bash
cp .env.example .env
```

Defaults work as-is for local development.

### 3. Start infrastructure

```bash
make infra
```

This runs `docker compose up -d --wait` and blocks until all health checks
pass. The `pubsub-emulator` image is ~1 GB and may take a minute on first pull.

### 4. Install Python dependencies

```bash
uv sync
```

### 5. Initialise emulator resources

Creates the Pub/Sub topic, the Pub/Sub subscription (required before the
subscriber can pull messages), the GCS bucket, and the PostgreSQL table:

```bash
make init-infra
```

This must be run before starting the subscriber or worker, and after any
`docker compose down` that removes volumes.

### 6. Add PDFs and load them into the database

The worker fetches PDF data from PostgreSQL (not from disk at runtime), so
PDFs must be seeded into the DB before firing events.

1. Drop one or more `.pdf` files into the `pdfs/` directory:

```bash
cp ~/some-file.pdf pdfs/
```

2. Seed the database — each file is upserted as a row in `pdf_documents`
   (keyed on filename, so re-running is safe):

```bash
make load-pdfs
```

Or directly:

```bash
uv run python scripts/load_pdfs.py
```

The script logs each upserted filename and a final count. Add or replace files
in `pdfs/` and re-run `make load-pdfs` at any time to refresh the DB.

## Running the pipeline

Open **four terminals**, each from the project root.

**Terminal 1 — taskiq worker**

```bash
make worker
# expands to:
# uv run taskiq worker cdc_pdf_pipeline.broker:broker cdc_pdf_pipeline.messaging.tasks
```

**Terminal 2 — Pub/Sub subscriber**

```bash
make subscriber
# expands to:
# uv run python -m cdc_pdf_pipeline.messaging.subscriber
```

**Terminal 3 — FastAPI**

```bash
make api
# expands to:
# uv run uvicorn cdc_pdf_pipeline.api:app --reload --port 8000
```

**Terminal 4 — publish test events**

Trigger a PDF merge using the defaults (table=documents, account=ACC-001,
document-type=contract):
```bash
make publish
```

Or with explicit values — **all four variables are required** if you override
any of them, otherwise the missing ones expand to empty strings and corrupt
the event payload:
```bash
make publish TABLE=documents OP=INSERT ACCOUNT=ACC-001 DOCTYPE=contract
```

Once published, the worker merges the PDFs and uploads to GCS. Verify the
upload happened by listing the bucket directly:
```bash
curl http://localhost:4443/storage/v1/b/pdf-pipeline-bucket/o
```

Then fetch via the API:
```bash
curl http://localhost:8000/documents/ACC-001/contract --output merged.pdf
open merged.pdf
```

Trigger a deletion:
```bash
uv run python -m cdc_pdf_pipeline.messaging.publisher \
    --table documents \
    --operation DELETE \
    --pk rec-001
```

Show all options:
```bash
uv run python -m cdc_pdf_pipeline.messaging.publisher --help
```

Visit `http://localhost:8000/docs` for the interactive Swagger UI.
## Project layout

```
cdc-pdf-pipeline/
├── docker-compose.yml          # Redis, Pub/Sub emulator, fake-gcs-server, Postgres
├── pyproject.toml              # uv / hatchling project
├── Makefile                    # infra, init-infra, api, worker, subscriber,
│                               #   publish, load-pdfs, format, lint, typecheck
├── .env.example                # copy to .env
├── pdfs/                       # drop source PDFs here, then run make load-pdfs
├── scripts/
│   ├── init_services.py        # creates Pub/Sub topic/sub, GCS bucket, DB tables
│   └── load_pdfs.py            # upserts pdfs/*.pdf into the pdf_documents table
└── cdc_pdf_pipeline/
    ├── config.py               # pydantic-settings (reads .env)
    ├── log.py                  # structlog setup: configure_logging(), get_logger()
    ├── broker.py               # taskiq ListQueueBroker + RedisAsyncResultBackend
    ├── api.py                  # FastAPI: GET /documents/{account_id}/{document_type}
    ├── db/
    │   ├── models.py           # SQLAlchemy ORM: Base, PdfDocument (bytea)
    │   └── ops.py              # engine, AsyncSessionLocal, get_session,
    │                           #   create_tables, fetch_all_pdfs, upsert_pdf
    ├── storage/
    │   ├── gcs.py              # GCS client (emulator-aware), upload_bytes,
    │                           #   blob_exists, stream_blob
    │   └── pdf.py              # collect_pdfs, merge_pdfs, merge_pdfs_from_bytes
    └── messaging/
        ├── tasks.py            # merge_and_upload_pdfs, handle_record_deletion
        ├── subscriber.py       # Pub/Sub streaming pull → dispatches tasks
        └── publisher.py        # CLI: publish mock CDC events
```

## Notes

- **Oracle BLOB → PostgreSQL `bytea`**: GoldenGate supports this. Ensure
  `SUPPLEMENTAL LOG DATA (LOB) COLUMNS` is enabled on the source table. Test
  with real data sizes — very large BLOBs (>500 MB) may need tuning.
- The `PDF_TABLES` set in `messaging/subscriber.py` controls which tables
  trigger a merge. Expand it to match your real source tables.
- In production, `fetch_all_pdfs()` in `db/ops.py` should be filtered by
  `account_id` / `document_type` (or whichever key GoldenGate replicates)
  rather than returning every row.
