"""Structured logging configuration with PII masking helpers."""

from __future__ import annotations

import logging
import re
import sys

_CONFIGURED = False

# Patterns that match PII values likely to appear in log messages.
# Each tuple: (compiled regex, replacement function).
_PII_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # Passport-like: alphanumeric 7-10 chars (prefixed by label)
    (re.compile(r"(?i)(passport[=:\s]+)([A-Z0-9]{7,10})"), r"\1***masked***"),
    # IBAN: 2-letter country + 2 check digits + up to 30 alphanum
    (re.compile(r"\b([A-Z]{2}\d{2})[A-Z0-9]{10,30}\b"), r"\1***masked***"),
    # E-mail: mask local part
    (re.compile(r"\b[\w.+-]+(@[\w.-]+\.\w+)\b"), r"***@masked***"),
]


class PIIMaskingFilter(logging.Filter):
    """Logging filter that redacts PII patterns from log messages."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Mask PII in the formatted message."""
        if record.args:
            # Format the message early so we can scrub it
            record.msg = record.getMessage()
            record.args = None
        msg = str(record.msg)
        for pattern, replacement in _PII_PATTERNS:
            msg = pattern.sub(replacement, msg)
        record.msg = msg
        return True


def configure_logging(level: str = "INFO") -> None:
    """Configure root logging once, writing structured lines to stdout."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(PIIMaskingFilter())
    formatter = logging.Formatter(
        fmt='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}'
    )
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a named logger."""
    return logging.getLogger(name)


def mask_secret(value: str | None, visible: int = 4) -> str:
    """Mask a sensitive value, keeping only the last `visible` characters.

    Used to avoid leaking PII (IBAN, passport, etc.) into logs (Rule #2).
    """
    if not value:
        return ""
    text = str(value)
    if len(text) <= visible:
        return "*" * len(text)
    return "*" * (len(text) - visible) + text[-visible:]
