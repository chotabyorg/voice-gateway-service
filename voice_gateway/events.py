"""Typed local facts at the future Conversation boundary.

These dataclasses are not wire DTOs and are never persisted by this service.
"""

from dataclasses import dataclass
from enum import Enum


class PlaybackState(str, Enum):
    NOT_STARTED = "not_started"
    STARTED = "started"
    PARTIAL = "partial"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"
    FAILED = "failed"
    UNKNOWN = "unknown"


class CallEndReason(str, Enum):
    DISCONNECTED = "disconnected"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass(frozen=True)
class CallStarted:
    call_id: str
    started_at_monotonic_ns: int
    caller_number: str = ""


@dataclass(frozen=True)
class FinalUtterance:
    call_id: str
    sequence: int
    text: str
    finalized_at_monotonic_ns: int


@dataclass(frozen=True)
class AuthorizedSpeech:
    """Speech text returned by the host Conversation port for playback."""

    request_id: str
    text: str
    # A partial (leased) sentence: the agent is still working on the same turn.
    is_partial: bool = False


@dataclass(frozen=True)
class PlaybackFact:
    call_id: str
    request_id: str
    state: PlaybackState
    transport_accepted_bytes: int
    generated_bytes: int
    occurred_at_monotonic_ns: int
    reason: str | None = None


@dataclass(frozen=True)
class CallEnded:
    call_id: str
    reason: CallEndReason
    ended_at_monotonic_ns: int
