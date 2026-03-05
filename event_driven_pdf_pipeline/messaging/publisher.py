"""
CLI for manually publishing mock CDC events to the Pub/Sub topic.

Usage examples:

  # Trigger a PDF merge
  uv run python -m event_driven_pdf_pipeline.pipeline.publisher \\
      --table documents --operation INSERT \\
      --account-id ACC-001 --document-type contract

  # Trigger a deletion
  uv run python -m event_driven_pdf_pipeline.pipeline.publisher \\
      --table documents --operation DELETE \\
      --pk rec-999
"""

import json
from typing import Annotated

import typer
from google.cloud import pubsub_v1

# config import also ensures PUBSUB_EMULATOR_HOST is set in os.environ
from event_driven_pdf_pipeline.config import settings

app = typer.Typer(help="Publish a mock CDC event to the Pub/Sub topic.")


@app.command()
def publish(
    table: Annotated[str, typer.Option(help="Table name")] = "documents",
    operation: Annotated[
        str, typer.Option(help="CDC operation: INSERT | UPDATE | DELETE")
    ] = "INSERT",
    account_id: Annotated[str, typer.Option(help="account_id field value")] = "ACC-001",
    document_type: Annotated[str, typer.Option(help="document_type field value")] = "contract",
    pk: Annotated[str, typer.Option(help="Primary key of the record")] = "rec-001",
) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(settings.pubsub_project_id, settings.pubsub_topic_id)

    payload = {
        "table": table,
        "operation": operation.upper(),
        "data": {
            "pk": pk,
            "account_id": account_id,
            "document_type": document_type,
        },
    }

    encoded = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic_path, encoded)
    message_id = future.result()

    typer.echo(f"Published to {topic_path}  message_id={message_id}")
    typer.echo(json.dumps(payload, indent=2))


if __name__ == "__main__":
    app()
