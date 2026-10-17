"""Explicitly injected candidate Conversation adapter; never a direct AI client."""

import asyncio
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal, NoReturn, TypeVar
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr, StrictBool

from voice_gateway.events import (
    AuthorizedSpeech,
    CallEnded,
    CallStarted,
    FinalUtterance,
    PlaybackFact,
    PlaybackState,
)
from voice_gateway.ports import StreamingConversationPort


class ConversationBoundaryError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Conversation boundary unavailable or invalid")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ConversationBoundaryError()
        result[key] = value
    return result


def _invalid_constant(value: str) -> NoReturn:
    raise ConversationBoundaryError()


@dataclass(frozen=True)
class TrustedVoiceBinding:
    """Platform-resolved binding; never infer it from audio/caller-supplied headers."""

    owner_id: str
    integration_id: UUID
    agent_id: UUID


class _Identity(BaseModel):
    model_config = ConfigDict(extra="forbid")
    call_id: UUID
    media_session_id: UUID


Response = TypeVar("Response", bound=_Identity)


class _Call(_Identity):
    conversation_id: UUID
    state: Literal["ACTIVE", "ENDED", "EXPIRED"]
    replayed: StrictBool


class _Accepted(_Identity):
    sequence: int = Field(strict=True, ge=1)
    message_id: UUID
    replayed: StrictBool


class _Candidate(_Identity):
    turn_id: UUID | None


class _Permission(_Identity):
    turn_id: UUID
    response_id: UUID
    send: StrictBool
    text: str | None = Field(default=None, min_length=1, max_length=16384)
    replayed: StrictBool
    is_partial: StrictBool = False


class _Fact(_Identity):
    response_id: UUID
    event_sequence: int = Field(strict=True, ge=1)
    replayed: StrictBool


@dataclass
class _Session:
    call_id: UUID
    media_session_id: UUID = field(default_factory=uuid4)
    failed: asyncio.Event = field(default_factory=asyncio.Event)
    heartbeat: asyncio.Task[None] | None = None
    response_id: UUID | None = None
    event_sequence: int = 0
    caller_number: str = ""
    evidence_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def identity(self) -> dict[str, str]:
        return {"call_id": str(self.call_id), "media_session_id": str(self.media_session_id)}


class ConversationHTTPPort(StreamingConversationPort):
    """Owns transient sessions only. Conversation owns messages, turns and send permission."""

    def __init__(
        self,
        base_url: str,
        token: SecretStr,
        resolve_binding: Callable[[str], Awaitable[TrustedVoiceBinding]],
        *,
        timeout: float = 3,
        poll_seconds: float = 0.1,
        heartbeat_seconds: float = 1,
        send_caller_number: bool = False,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        url = urlsplit(base_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
            or not base_url.isascii()
            or any(ord(c) <= 32 for c in base_url)
            or not 0 < timeout <= 5
            or not 0.05 <= poll_seconds <= 1
            or not 0.25 <= heartbeat_seconds <= 1
        ):
            raise ValueError("Invalid Conversation adapter configuration")
        raw = token.get_secret_value()
        if not 32 <= len(raw) <= 256 or any(
            c not in ("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._~-")
            for c in raw
        ):
            raise ValueError("Invalid Conversation service credential")
        self.client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"X-Service-Token": raw},
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
            transport=transport,
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
        )
        self.resolve_binding = resolve_binding
        self.timeout, self.poll_seconds = timeout, poll_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.send_caller_number = send_caller_number
        self.sessions: dict[str, _Session] = {}

    async def _request(
        self, session: _Session, path: str, response_type: type[Response], **payload: object
    ) -> Response:
        try:
            async with asyncio.timeout(self.timeout):
                async with self.client.stream(
                    "POST",
                    "/internal/v1/voice/" + path,
                    json={**session.identity(), **payload},
                ) as response:
                    if response.status_code != 200:
                        raise ConversationBoundaryError()
                    media = response.headers.get_list("content-type")
                    encoding = response.headers.get_list("content-encoding")
                    if (
                        len(media) != 1
                        or media[0].split(";", 1)[0].strip().lower() != "application/json"
                        or any(v.lower() != "identity" for v in encoding)
                    ):
                        raise ConversationBoundaryError()
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(body) + len(chunk) > 65536:
                            raise ConversationBoundaryError()
                        body.extend(chunk)
                result = response_type.model_validate(
                    json.loads(
                        bytes(body),
                        object_pairs_hook=_unique_object,
                        parse_constant=_invalid_constant,
                    )
                )
                if (
                    result.call_id != session.call_id
                    or result.media_session_id != session.media_session_id
                ):
                    raise ConversationBoundaryError()
                return result
        except asyncio.CancelledError:
            raise
        except Exception:
            session.failed.set()
            raise ConversationBoundaryError() from None

    def _session(self, call_id: str) -> _Session:
        session = self.sessions.get(str(call_id))
        if session is None:
            raise ConversationBoundaryError()
        return session

    @staticmethod
    def _valid_caller_number(value: object) -> str:
        return value if isinstance(value, str) and re.fullmatch(r"\+375[0-9]{9}", value) else ""

    async def call_started(self, event: CallStarted) -> None:
        if event.call_id in self.sessions or len(self.sessions) >= 128:
            raise ConversationBoundaryError()
        session = _Session(UUID(event.call_id))
        session.caller_number = self._valid_caller_number(event.caller_number)
        self.sessions[event.call_id] = session
        try:
            async with asyncio.timeout(self.timeout):
                binding = await self.resolve_binding(event.call_id)
            payload: dict[str, object] = {
                "owner_id": binding.owner_id,
                "integration_id": str(binding.integration_id),
                "agent_id": str(binding.agent_id),
            }
            if self.send_caller_number and session.caller_number:
                # Only a validated ANI is forwarded, and only when the platform
                # side already accepts the field.
                payload["caller_number"] = session.caller_number
            result = await self._request(session, "calls/start", _Call, **payload)
            if result.state != "ACTIVE" or result.replayed:
                raise ConversationBoundaryError()
            session.heartbeat = asyncio.create_task(
                self._heartbeat(session), name="voice-call-lease"
            )
        except BaseException:
            self.sessions.pop(event.call_id, None)
            # A lost start response may have committed. End is idempotent and cannot
            # target another media session, even if the start did not commit.
            try:
                await self._request(session, "calls/end", _Call, reason="FAILED")
            except (Exception, asyncio.CancelledError):
                pass
            raise

    async def _heartbeat(self, session: _Session) -> None:
        try:
            while True:
                await asyncio.sleep(self.heartbeat_seconds)
                result = await self._request(session, "calls/heartbeat", _Call)
                if result.state != "ACTIVE":
                    raise ConversationBoundaryError()
        except asyncio.CancelledError:
            raise
        except Exception:
            session.failed.set()

    async def wait_failure(self, call_id: str) -> NoReturn:
        await self._session(call_id).failed.wait()
        raise ConversationBoundaryError()

    async def final_utterance(self, event: FinalUtterance) -> AuthorizedSpeech | None:
        session = self._session(event.call_id)
        result = await self._request(
            session,
            "utterances",
            _Accepted,
            sequence=event.sequence,
            text=event.text,
        )
        if result.sequence != event.sequence:
            session.failed.set()
            raise ConversationBoundaryError()
        # Admission is separate from output. New input can fence an in-flight answer.
        return None

    async def responses(self, call_id: str) -> AsyncIterator[AuthorizedSpeech]:
        session = self._session(call_id)
        while not session.failed.is_set():
            candidate = await self._request(session, "responses/next", _Candidate)
            if candidate.turn_id is None:
                await asyncio.sleep(self.poll_seconds)
                continue
            permission = await self._request(
                session,
                "responses/begin",
                _Permission,
                turn_id=str(candidate.turn_id),
            )
            if permission.turn_id != candidate.turn_id or permission.send != (
                permission.text is not None
            ):
                session.failed.set()
                raise ConversationBoundaryError()
            if permission.send:
                if permission.replayed or permission.text is None:
                    session.failed.set()
                    raise ConversationBoundaryError()
                session.response_id, session.event_sequence = permission.response_id, 0
                yield AuthorizedSpeech(
                    str(permission.response_id),
                    permission.text,
                    is_partial=bool(permission.is_partial),
                )
        raise ConversationBoundaryError()

    async def playback_fact(self, event: PlaybackFact) -> None:
        session = self._session(event.call_id)
        async with session.evidence_lock:
            if str(session.response_id) != event.request_id:
                session.failed.set()
                raise ConversationBoundaryError()
            sequence = session.event_sequence + 1
            result = await self._request(
                session,
                "playback",
                _Fact,
                response_id=event.request_id,
                event_sequence=sequence,
                state=event.state.value.upper(),
                generated_bytes=event.generated_bytes,
                transport_accepted_bytes=event.transport_accepted_bytes,
                terminal=event.state not in {PlaybackState.STARTED, PlaybackState.PARTIAL},
            )
            if result.response_id != session.response_id or result.event_sequence != sequence:
                session.failed.set()
                raise ConversationBoundaryError()
            session.event_sequence = sequence

    async def call_ended(self, event: CallEnded) -> None:
        session = self.sessions.pop(event.call_id, None)
        if session is None:
            return
        if session.heartbeat is not None:
            session.heartbeat.cancel()
            await asyncio.gather(session.heartbeat, return_exceptions=True)
        await self._request(session, "calls/end", _Call, reason=event.reason.value.upper())

    async def close(self) -> None:
        # The runtime owns ending calls first. A process crash is bounded by the lease.
        for session in self.sessions.values():
            session.failed.set()
            if session.heartbeat is not None:
                session.heartbeat.cancel()
        await asyncio.gather(
            *(s.heartbeat for s in self.sessions.values() if s.heartbeat is not None),
            return_exceptions=True,
        )
        self.sessions.clear()
        await self.client.aclose()
