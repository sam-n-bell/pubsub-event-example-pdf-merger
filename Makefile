.PHONY: infra init api worker subscriber publish load-pdfs format lint typecheck

infra:
	docker compose up -d --wait

init-infra: infra
	uv run python scripts/init_services.py

api:
	uv run uvicorn cdc_pdf_pipeline.api:app --reload --port 8000

worker:
	uv run taskiq worker cdc_pdf_pipeline.broker:broker cdc_pdf_pipeline.messaging.tasks

subscriber:
	uv run python -m cdc_pdf_pipeline.messaging.subscriber

load-pdfs:
	uv run python scripts/load_pdfs.py

format:
	uv run ruff format .

lint:
	uv run ruff check .

typecheck:
	uv run mypy cdc_pdf_pipeline/

# Defaults — override any on the command line:
#   make publish TABLE=accounts OP=UPDATE ACCOUNT=XYZ DOCTYPE=invoice
TABLE    ?= documents
OP       ?= INSERT
ACCOUNT  ?= ACC-001
DOCTYPE  ?= contract

publish:
	uv run python -m cdc_pdf_pipeline.messaging.publisher \
		--table $(TABLE) \
		--operation $(OP) \
		--account-id $(ACCOUNT) \
		--document-type $(DOCTYPE)
