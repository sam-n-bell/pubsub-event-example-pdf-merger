"""
Structured logging via structlog.

Usage in any module:
    from event_driven_pdf_pipeline.log import get_logger
    logger = get_logger(__name__)
    logger.info("event_name", key=value, other_key=other_value)

Call configure_logging() once at each process entry point (API lifespan,
subscriber startup, worker on_startup hook, script main()).

Set JSON_LOGS=true for JSON output (production/log aggregators).
Leave unset (or set to false) for pretty coloured console output in dev.
"""

import logging
import os
from typing import Any

import structlog


def configure_logging() -> None:
    """
    Wire structlog to the stdlib root logger.

    Uses ProcessorFormatter so that log records emitted by third-party
    libraries (google-cloud, taskiq, uvicorn, …) are also rendered through
    the structlog pipeline.
    """
    json_logs = os.getenv("JSON_LOGS", "false").lower() == "true"

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    renderer: Any = (
        structlog.processors.JSONRenderer() if json_logs else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger bound to the given name."""
    return structlog.get_logger(name)
