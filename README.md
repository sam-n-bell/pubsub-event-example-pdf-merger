# cdc-pdf-pipeline

Demo of a CDC-driven PDF processing pipeline. Mocked infrastructure runs in
Docker; the Python processes run on the host via `uv`.

## Architecture

```
                    ┌──────────────────────────────────────────┐
                    │          Docker Compose                   │
   publisher CLI    │  ┌──────────────┐  ┌──────────────────┐  │
   (manual test) ──▶│  │ Pub/Sub emu  │  │ fake-gcs-server  │  │
                    │  │  :8085       │  │  :4443 (HTTP)    │  │
                    │  └──────┬───────┘  └────────▲─────────┘  │
                    │         │                   │            │
                    │  ┌──────▼───────┐           │            │
                    │  │    Redis     │           │            │
                    │  │  :6379       │           │            │
                    │  └──────────────┘           │            │
                    └──────────────────────────────────────────┘
                           │  (taskiq queue)      │ (GCS upload/fetch)
                    ┌──────▼──────┐        ┌──────┴──────┐
                    │ subscriber  │        │   worker    │
                    │  (host)     │──kiq──▶│  (host)     │
                    └─────────────┘        └─────────────┘
                                                          ▲
                    ┌─────────────┐                       │
                    │  FastAPI    │─── GET /documents ────┘
                    │  :8000      │
                    └─────────────┘
```

**CDC event → task routing:**

| table      | operation       | task                    |
|------------|-----------------|-------------------------|
| documents  | INSERT / UPDATE | `merge_and_upload_pdfs` |
| attachments| INSERT / UPDATE | `merge_and_upload_pdfs` |
| files      | INSERT / UPDATE | `merge_and_upload_pdfs` |
| *any*      | DELETE          | `handle_record_deletion`|

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
docker compose up -d
```

Wait until all three services are healthy (check with `docker compose ps`).
The `pubsub-emulator` image is ~1 GB and may take a minute on first pull.

### 4. Install Python dependencies

```bash
uv sync
```

### 5. Initialise emulator resources

Creates the Pub/Sub topic + subscription and the GCS bucket:

```bash
uv run python scripts/init_services.py
```

### 6. Drop some PDFs into the `pdfs/` directory

The worker merges everything it finds in `pdfs/*.pdf` for every event.
Put 2–4 PDF files there before firing events.

## Running the pipeline

Open **four terminals**, each from the project root.

**Terminal 1 — taskiq worker**

```bash
uv run taskiq worker cdc_pdf_pipeline.broker:broker cdc_pdf_pipeline.tasks
```

**Terminal 2 — Pub/Sub subscriber**

```bash
uv run python -m cdc_pdf_pipeline.subscriber
```

**Terminal 3 — FastAPI**

```bash
uv run uvicorn cdc_pdf_pipeline.api:app --reload --port 8000
```

**Terminal 4 — publish test events**

Trigger a PDF merge:
```bash
uv run python -m cdc_pdf_pipeline.publisher \
    --table documents \
    --operation INSERT \
    --account-id ACC-001 \
    --document-type contract
```

Trigger a deletion:
```bash
uv run python -m cdc_pdf_pipeline.publisher \
    --table documents \
    --operation DELETE \
    --pk rec-001
```

Show all options:
```bash
uv run python -m cdc_pdf_pipeline.publisher --help
```

## Fetching the merged PDF

Once the worker has uploaded the file:

```bash
# In a browser or curl:
curl http://localhost:8000/documents/ACC-001/contract --output merged.pdf
open merged.pdf
```

Or visit `http://localhost:8000/docs` for the interactive Swagger UI.

## Project layout

```
cdc-pdf-pipeline/
├── docker-compose.yml          # Redis, Pub/Sub emulator, fake-gcs-server
├── pyproject.toml              # uv / hatchling project file
├── .env.example                # copy to .env
├── pdfs/                       # drop source PDFs here
└── src/
    └── cdc_pdf_pipeline/
        ├── config.py           # pydantic-settings (reads .env)
        ├── broker.py           # taskiq Redis broker
        ├── gcs.py              # GCS client factory (emulator-aware)
        ├── tasks.py            # merge_and_upload_pdfs, handle_record_deletion
        ├── subscriber.py       # Pub/Sub streaming pull → dispatches tasks
        ├── publisher.py        # CLI: publish mock CDC events
        └── api.py              # FastAPI: GET /documents/{account_id}/{document_type}
```

## Notes

- **Oracle BLOB → PostgreSQL `bytea`**: GoldenGate supports this. Ensure
  `SUPPLEMENTAL LOG DATA (LOB) COLUMNS` is enabled on the source table. Test
  with real data sizes — very large BLOBs (>500 MB) may need tuning.
- The subscriber's `PDF_TABLES` set in `subscriber.py` controls which tables
  trigger a merge. Expand it to match your three real tables.
- In production replace `pdfs/` scanning with a DB query that fetches the four
  related rows for the changed record.
