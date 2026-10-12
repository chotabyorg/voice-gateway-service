"""One transient media session; no canonical conversation or AgentTurn state."""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum

from voice_gateway.events import (
    CallEndReason,
    CallEnded,
    CallStarted,
    FinalUtterance,
    PlaybackFact,
    PlaybackState,
)
from voice_gateway.media.audio import rms, validate_pcm
from voice_gateway.media.completion import bounded_call, finish_owned_task
from voice_gateway.media.interruption import InterruptionDetector, InterruptionSettings
from voice_gateway.media.interfaces import TransportIdleTimeout
from voice_gateway.media.playback import play_pcm

from .utterances import UtteranceCoordinator

logger = logging.getLogger(__name__)


class CallState(str, Enum):
    CREATED = "created"
    PREPARING = "preparing"
    ACTIVE = "active"
    DRAINING = "draining"
    CLOSED = "closed"


@dataclass(frozen=True)
class MediaSettings:
    speech_threshold: int = 500
    turn_silence_seconds: float = 0.5
    continuation_seconds: float = 0.9
    post_playback_suppression_seconds: float = 0.8
    barge_in: bool = True
    interruption: InterruptionSettings = field(default_factory=InterruptionSettings)
    max_call_seconds: float = 3600
    cleanup_timeout: float = 6
    max_tts_chars: int = 4000
    max_playback_bytes: int = 8 * 1024 * 1024

    def __post_init__(self):
        if (
            self.speech_threshold < 1
            or self.turn_silence_seconds < 0
            or self.continuation_seconds < 0
            or self.post_playback_suppression_seconds < 0
            or self.max_call_seconds <= 0
            or self.cleanup_timeout <= 0
            or self.max_tts_chars < 1
            or self.max_playback_bytes < 2
        ):
            raise ValueError("Invalid media call settings")


class MediaCall:
    """One-shot owner of injected resources and short-lived media coordination.

    The typed Conversation port is local-only. Transport write completion is
    delivery to the transport, never proof that the remote caller heard audio.
    """

    def __init__(
        self, transport, stt, tts, conversation_port, *, settings=MediaSettings(), emit=None
    ):
        self.transport, self.stt, self.tts = transport, stt, tts
        self.conversation_port, self.settings = conversation_port, settings
        self._observer = emit
        self.state = CallState.CREATED
        self._sequence = 0
        self._call_started = False
        self._end_reason = CallEndReason.DISCONNECTED
        self.utterances = UtteranceCoordinator(
            self._deliver_utterance,
            silence_seconds=settings.turn_silence_seconds,
            continuation_seconds=settings.continuation_seconds,
        )
        self.detector = InterruptionDetector(settings.interruption)
        self._speech_lock = asyncio.Lock()
        self._output_task = None
        self._interrupt_reason = None
        self._output_started = 0.0
        self._suppress_until = 0.0
        self._generated_bytes = 0
        self._accepted_bytes = 0
        self._playback_started = False
        self._last_progress_bytes = 0

    def emit(self, event, **values):
        logger.info(event, extra={"event": event, **values})
        if self._observer is not None:
            try:
                self._observer(event, **values)
            except Exception as exc:
                logger.warning(
                    "media_observer_failed",
                    extra={
                        "event": "media_observer_failed",
                        "error_type": type(exc).__name__,
                    },
                )

    def _state(self, value):
        self.state = value
        self.emit("media_call_state", state=value.value)

    async def run(self):
        if self.state != CallState.CREATED:
            raise RuntimeError("MediaCall can run only once")
        self._state(CallState.PREPARING)
        cancellation = None
        try:
            async with asyncio.timeout(self.settings.max_call_seconds):
                await self.transport.prepare()
                await self.stt.start_stream(on_final=self._final)
                await self.conversation_port.call_started(
                    CallStarted(
                        call_id=self._call_id(),
                        started_at_monotonic_ns=time.monotonic_ns(),
                    )
                )
                self._call_started = True
                self._state(CallState.ACTIVE)
                while True:
                    try:
                        audio = await self.transport.read_audio_chunk()
                    except TransportIdleTimeout:
                        self._end_reason = CallEndReason.TIMEOUT
                        break
                    if audio is None:
                        self._end_reason = CallEndReason.DISCONNECTED
                        break
                    if audio:
                        await self.feed_audio(audio)
        except asyncio.CancelledError as exc:
            cancellation = exc
            self._end_reason = CallEndReason.CANCELLED
            raise
        except TimeoutError:
            self._end_reason = CallEndReason.TIMEOUT
            raise
        except Exception:
            self._end_reason = CallEndReason.FAILED
            raise
        finally:
            self._state(CallState.DRAINING)
            cleanup = asyncio.create_task(self._cleanup(), name="voice-call-cleanup")
            try:
                await finish_owned_task(cleanup, cancellation=cancellation)
            finally:
                self._state(CallState.CLOSED)
                if self._call_started:
                    await self._notify_call_ended()

    def _call_id(self):
        return getattr(self.transport, "call_id", "") or "transport-pending"

    async def _notify_call_ended(self):
        event = CallEnded(self._call_id(), self._end_reason, time.monotonic_ns())
        self.emit("voice_call_ended", call_end_reason=self._end_reason.value)
        try:
            await self.conversation_port.call_ended(event)
        except Exception as exc:
            self.emit(
                "conversation_port_error", operation="call_ended", error_type=type(exc).__name__
            )

    async def _final(self, text):
        if self.state == CallState.ACTIVE:
            await self.utterances.submit(text)

    async def _deliver_utterance(self, text):
        if self.state != CallState.ACTIVE:
            return
        self._sequence += 1
        event = FinalUtterance(
            call_id=self._call_id(),
            sequence=self._sequence,
            text=text,
            finalized_at_monotonic_ns=time.monotonic_ns(),
        )
        self.emit("voice_final_utterance", sequence=event.sequence, text_chars=len(text))
        try:
            authorized_speech = await self.conversation_port.final_utterance(event)
        except Exception as exc:
            self.emit(
                "conversation_port_error",
                operation="final_utterance",
                error_type=type(exc).__name__,
            )
            return
        if authorized_speech is not None:
            try:
                await self.speak(authorized_speech.request_id, authorized_speech.text)
            except Exception as exc:
                self.emit("media_speech_error", error_type=type(exc).__name__)

    async def feed_audio(self, audio):
        validate_pcm(audio)
        if self._output_task is not None:
            if not self.settings.barge_in:
                return
            event = self.detector.feed(audio, elapsed=time.monotonic() - self._output_started)
            if event is None:
                return
            await self.interrupt(reason="barge_in")
            self.utterances.speech()
            for chunk in event.chunks:
                await self.stt.feed_audio(chunk)
            return
        if time.monotonic() < self._suppress_until:
            return
        if rms(audio) >= self.settings.speech_threshold:
            self.utterances.speech()
        await self.stt.feed_audio(audio)

    async def speak(self, request_id, text):
        """Play only locally authorized text; return False when barge-in wins."""
        async with self._speech_lock:
            if self.state != CallState.ACTIVE:
                raise RuntimeError("Cannot speak outside an active media call")
            text = text.strip()
            if not text:
                return True
            self._generated_bytes = self._accepted_bytes = 0
            self._playback_started = False
            self._last_progress_bytes = 0
            if len(text) > self.settings.max_tts_chars:
                await self._playback_fact(request_id, PlaybackState.FAILED, "text_limit")
                raise ValueError("Speech text exceeds configured limit")
            self.detector.reset()
            self.detector.echo_rms = 0
            self._output_started = time.monotonic()
            self._interrupt_reason = None
            task = asyncio.create_task(self._produce(request_id, text), name="voice-playback")
            self._output_task = task
            interrupted = False
            try:
                await task
                return True
            except asyncio.CancelledError:
                interrupted = self._interrupt_reason == "barge_in"
                if interrupted:
                    state = (
                        PlaybackState.INTERRUPTED
                        if self._accepted_bytes
                        else PlaybackState.NOT_STARTED
                    )
                    await self._playback_fact(request_id, state, "barge_in")
                    return False
                if self._interrupt_reason == "disconnect":
                    state = (
                        PlaybackState.UNKNOWN if self._accepted_bytes else PlaybackState.NOT_STARTED
                    )
                    await self._playback_fact(request_id, state, "disconnect")
                else:
                    state = (
                        PlaybackState.UNKNOWN if self._accepted_bytes else PlaybackState.NOT_STARTED
                    )
                    await self._playback_fact(request_id, state, "cancelled")
                if asyncio.current_task().cancelling():
                    raise
                return False
            finally:
                self._output_task = None
                self.detector.reset()
                self._suppress_until = time.monotonic() + (
                    0
                    if interrupted or not self._accepted_bytes
                    else self.settings.post_playback_suppression_seconds
                )

    async def _produce(self, request_id, text):
        transport_failure_reported = False
        try:
            audio = await self.tts.synthesize(text)
            validate_pcm(audio)
            self._generated_bytes = len(audio)
            if self._generated_bytes > self.settings.max_playback_bytes:
                raise ValueError("Generated audio exceeds configured limit")
            if text and not audio:
                raise ValueError("TTS returned empty audio")

            async def write(chunk):
                nonlocal transport_failure_reported
                try:
                    await self.transport.write_audio_chunk(chunk)
                except Exception as exc:
                    transport_failure_reported = True
                    await self._playback_fact(
                        request_id, PlaybackState.UNKNOWN, f"transport_{type(exc).__name__}"
                    )
                    raise
                self._accepted_bytes += len(chunk)
                state = (
                    PlaybackState.STARTED if not self._playback_started else PlaybackState.PARTIAL
                )
                self._playback_started = True
                if (
                    state == PlaybackState.STARTED
                    or self._accepted_bytes - self._last_progress_bytes >= 8000
                ):
                    self._last_progress_bytes = self._accepted_bytes
                    await self._playback_fact(request_id, state)

            result = await play_pcm(audio, write)
            if result.complete and self._accepted_bytes == self._generated_bytes:
                await self._playback_fact(request_id, PlaybackState.COMPLETED)
            else:
                await self._playback_fact(request_id, PlaybackState.UNKNOWN, "incomplete_transport")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not transport_failure_reported:
                await self._playback_fact(request_id, PlaybackState.FAILED, type(exc).__name__)
            raise

    async def _playback_fact(self, request_id, state, reason=None):
        event = PlaybackFact(
            call_id=self._call_id(),
            request_id=request_id,
            state=state,
            transport_accepted_bytes=self._accepted_bytes,
            generated_bytes=self._generated_bytes,
            occurred_at_monotonic_ns=time.monotonic_ns(),
            reason=reason,
        )
        self.emit(
            "voice_playback_fact",
            playback_state=state.value,
            transport_accepted_bytes=self._accepted_bytes,
            generated_bytes=self._generated_bytes,
        )
        try:
            await self.conversation_port.playback_fact(event)
        except Exception as exc:
            self.emit(
                "conversation_port_error", operation="playback_fact", error_type=type(exc).__name__
            )

    async def interrupt(self, *, reason="barge_in"):
        task = self._output_task
        if task is not None and not task.done():
            self._interrupt_reason = reason
            task.cancel()
        try:
            await self.transport.clear_output()
        finally:
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)

    async def _cleanup(self):
        # Close transport before draining STT so disconnect cannot leave a live read.
        resources = (
            ("output", lambda: self.interrupt(reason="disconnect")),
            ("utterances", self.utterances.close),
            ("transport", self.transport.close),
            ("stt", self.stt.stop_stream),
            ("tts", self.tts.close),
        )
        cancellation = None
        for name, close in resources:
            try:
                completed = await bounded_call(
                    close(),
                    timeout=self.settings.cleanup_timeout,
                    cancellation_grace=min(0.1, self.settings.cleanup_timeout),
                )
                if not completed:
                    self.emit("media_cleanup_timeout", resource=name)
            except asyncio.CancelledError as exc:
                cancellation = cancellation or exc
                self.emit("media_cleanup_error", resource=name, error_type=type(exc).__name__)
            except Exception as exc:
                self.emit("media_cleanup_error", resource=name, error_type=type(exc).__name__)
        if cancellation is not None:
            raise cancellation
