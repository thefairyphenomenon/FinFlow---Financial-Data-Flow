"""
core/logger.py
--------------
Structured logging setup for DataFlow.

WHY STRUCTURED LOGGING:
- In production, logs go to systems like Datadog, Splunk, ELK Stack
- Structured (JSON) logs are machine-parseable and searchable
- You can query: "show me all errors from the ingestion module in the last hour"
"""

import logging
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from core.settings import settings


class JSONFormatter(logging.Formatter):
    """
    Formats log records as JSON lines.
    Each log entry is a complete JSON object — easy to ingest into log aggregators.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }

        # Attach exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Attach any extra fields passed to the logger
        # e.g. logger.info("msg", extra={"ticker": "AAPL"})
        for key, val in record.__dict__.items():
            if key not in (
                "args", "asctime", "created", "exc_info", "exc_text",
                "filename", "funcName", "levelname", "levelno", "lineno",
                "message", "module", "msecs", "msg", "name", "pathname",
                "process", "processName", "relativeCreated", "stack_info",
                "thread", "threadName",
            ):
                log_entry[key] = val

        return json.dumps(log_entry)


def get_logger(name: str) -> logging.Logger:
    """
    Factory function — every module gets its own named logger.
    Usage: logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # Already configured, avoid duplicate handlers

    logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))

    # Console handler (stdout) — captured by Docker and log aggregators
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(JSONFormatter())
    logger.addHandler(console_handler)

    # File handler — keeps local logs for debugging
    log_dir = Path("data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_dir / "dataflow.log")
    file_handler.setFormatter(JSONFormatter())
    logger.addHandler(file_handler)

    return logger
