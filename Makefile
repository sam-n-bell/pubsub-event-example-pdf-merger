.PHONY: infra init api worker subscriber publish

infra:
	docker compose up -d

init-infra:
	uv run python scripts/init_services.py

api:
	uv run uvicorn cdc_pdf_pipeline.api:app --reload --port 8000

worker:
	uv run taskiq worker cdc_pdf_pipeline.broker:broker cdc_pdf_pipeline.tasks

subscriber:
	uv run python -m cdc_pdf_pipeline.subscriber

# Usage: make publish TABLE=documents OP=INSERT ACCOUNT=ACC-001 DOCTYPE=contract
publish:
	uv run python -m cdc_pdf_pipeline.publisher \
		--table $(TABLE) \
		--operation $(OP) \
		--account-id $(ACCOUNT) \
		--document-type $(DOCTYPE)
