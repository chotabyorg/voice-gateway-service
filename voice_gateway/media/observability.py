"""Metadata-only media diagnostics; observers cannot break the audio path."""

import logging

logger = logging.getLogger(__name__)


def emit_event(event, *, observer=None, **values):
    logger.info(event, extra={"event": event, **values})
    if observer is not None:
        try:
            observer(event, **values)
        except Exception as exc:
            logger.warning(
                "media_observer_failed",
                extra={"event": "media_observer_failed", "error_type": type(exc).__name__},
            )
