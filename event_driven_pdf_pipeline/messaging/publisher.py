"""
CLI for manually publishing mock GoldenGate CDC events to the Pub/Sub topic.

Usage examples:

  # Trigger a PDF merge (INSERT)
  uv run python -m event_driven_pdf_pipeline.messaging.publisher \\
      --table documents --operation INSERT \\
      --account-id ACC-001 --document-type contract

  # Trigger a deletion
  uv run python -m event_driven_pdf_pipeline.messaging.publisher \\
      --table documents --operation DELETE \\
      --pk rec-999
"""

from typing import Annotated

import typer
from google.cloud import pubsub_v1

# config import also ensures PUBSUB_EMULATOR_HOST is set in os.environ
from event_driven_pdf_pipeline.config import settings
from event_driven_pdf_pipeline.messaging.schemas import CdcEvent, CdcRowData, OpType

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

    op = OpType(operation.upper())  # normalise_op_type validator handles long-form too
    row = CdcRowData(id=pk, account_id=account_id, document_type=document_type)
    event = CdcEvent(
        table=table,
        op_type=op,
        primary_keys=["id"],
        before=row if op == OpType.DELETE else None,
        after=row if op != OpType.DELETE else None,
    )

    encoded = event.model_dump_json().encode("utf-8")
    future = publisher.publish(topic_path, encoded)
    message_id = future.result()

    typer.echo(f"Published to {topic_path}  message_id={message_id}")
    typer.echo(event.model_dump_json(indent=2))


if __name__ == "__main__":
    app()
