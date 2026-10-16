"""JSON logs with an explicit field allowlist; media text and credentials stay out."""

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    _fields = (
        "event",
        "service",
        "mode",
        "state",
        "resource",
        "error_type",
        "call_count",
        "active_calls",
        "audiosocket_port",
        "text_chars",
        "audio_ms",
        "speech_ms",
        "silence_ms",
        "endpoint_reason",
        "audio_bytes",
        "audio_rms_max",
        "input_gain",
        "queue_ms",
        "duration_ms",
        "jitter_ms",
        "pending_tasks",
        "operation",
        "provider",
        "model",
        "language",
        "bytes",
        "generated_bytes",
        "transport_accepted_bytes",
        "sequence",
        "playback_state",
        "call_end_reason",
    )

    def format(self, record):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
        }
        for name in self._fields:
            value = getattr(record, name, None)
            if value is not None and isinstance(value, (str, int, float, bool)):
                payload[name] = value
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)


def configure_logging():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
