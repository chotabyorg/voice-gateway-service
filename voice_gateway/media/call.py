"""One transient media session; no canonical conversation or AgentTurn state."""

import asyncio
import logging
import time
from contextlib import aclosing
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
from voice_gateway.media.audio import PCM, rms, validate_pcm
from voice_gateway.media.completion import bounded_call, finish_owned_task, observe_task
from voice_gateway.media.farewell import is_farewell, stock_opening_key
from voice_gateway.media.providers.greeting import has_meaningful_speech
from voice_gateway.media.interruption import InterruptionDetector, InterruptionSettings
from voice_gateway.media.interfaces import TransportIdleTimeout
from voice_gateway.media.playback import PlaybackControl, play_pcm_stream

from .utterances import UtteranceCoordinator

logger = logging.getLogger(__name__)

IDLE_MARKER = "[silence]"

# How long an admitted caller utterance may keep the agent "thinking" before the
# idle machinery is allowed to nudge or end the call again. A slow model reply is
# still an answer in progress, not silence.
RESPONSE_PENDING_SECONDS = 30.0


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
    numeric_continuation_seconds: float = 2.5
    post_playback_suppression_seconds: float = 0
    barge_in: bool = True
    interruption: InterruptionSettings = field(default_factory=InterruptionSettings)
    max_call_seconds: float = 3600
    cleanup_timeout: float = 6
    max_tts_chars: int = 4000
    max_pending_finals: int = 8
    max_pending_final_chars: int = 16000
    max_playback_bytes: int = 8 * 1024 * 1024
    interrupt_timeout: float = 0.2
    smooth_barge_in: bool = True
    hangup_on_farewell: bool = True
    farewell_grace_seconds: float = 0.3
    idle_prompt_seconds: float = 0.0
    max_idle_prompts: int = 2

    def __post_init__(self):
        if (
            self.speech_threshold < 1
            or self.turn_silence_seconds < 0
            or self.continuation_seconds < 0
            or self.numeric_continuation_seconds < self.continuation_seconds
            or self.post_playback_suppression_seconds < 0
            or self.max_call_seconds <= 0
            or self.cleanup_timeout <= 0
            or self.max_tts_chars < 1
            or self.max_pending_finals < 1
            or self.max_pending_final_chars < 1
            or self.max_playback_bytes < 2
            or self.interrupt_timeout <= 0
            or not 0 <= self.farewell_grace_seconds <= 10
            or self.idle_prompt_seconds < 0
            or self.max_idle_prompts < 0
        ):
            raise ValueError("Invalid media call settings")


@dataclass
class _Output:
    request_id: str
    generated: int = 0
    accepted: int = 0
    last_progress: int = 0
    writing: bool = False
    transport_failed: bool = False
    reason: str | None = None
    revoked: asyncio.Event = field(default_factory=asyncio.Event)
    control: PlaybackControl = field(default_factory=PlaybackControl)
    started_at: float = field(default_factory=time.monotonic)
    speech_ended_at: float | None = None


class MediaCall:
    """One-shot owner of injected resources and short-lived media coordination.

    The typed Conversation port is local-only. Transport write completion is
    delivery to the transport, never proof that the remote caller heard audio.
    """

    def __init__(
        self,
        transport,
        stt,
        tts,
        conversation_port,
        *,
        settings=MediaSettings(),
        emit=None,
        interruption_classifier=None,
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
            numeric_continuation_seconds=settings.numeric_continuation_seconds,
            max_pending=settings.max_pending_finals,
            max_pending_chars=settings.max_pending_final_chars,
            emit=emit,
        )
        self.detector = InterruptionDetector(
            settings.interruption, classifier=interruption_classifier
        )
        self._speech_lock = asyncio.Lock()
        self._output_task = None
        self._output = None
        self._retired_outputs = set()
        self._clear_output_task = None
        self._interrupt_count = 0
        self._interrupt_ready = asyncio.Event()
        self._interrupt_ready.set()
        self._output_started = 0.0
        self._suppress_until = 0.0
        self._activity_pending = b""
        self._response_task = None
        self._boundary_failure_task = None
        self._end_call = asyncio.Event()
        self._end_call_task = None
        self._caller_farewell = False
        self._spoken_openings: set[str] = set()
        self._idle_task = None
        self._idle_nudges = 0
        self._last_prompt_at = 0.0
        # When the caller's latest utterance was admitted and no answer has been
        # spoken yet: the agent is still preparing a reply, so the caller must
        # not be nudged or hung up on.
        self._awaiting_since: float | None = None

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
                if hasattr(self.tts, "prewarm"):
                    self.tts.prewarm()
                await self.stt.start_stream(on_final=self._final)
                await self.conversation_port.call_started(
                    CallStarted(
                        call_id=self._call_id(),
                        started_at_monotonic_ns=time.monotonic_ns(),
                        caller_number=getattr(self.transport, "caller_number", "") or "",
                    )
                )
                self._call_started = True
                self._state(CallState.ACTIVE)
                if hasattr(self.conversation_port, "responses"):
                    self._response_task = asyncio.create_task(
                        self._receive_responses(), name="voice-conversation-responses"
                    )
                if hasattr(self.conversation_port, "wait_failure"):
                    self._boundary_failure_task = asyncio.create_task(
                        self.conversation_port.wait_failure(self._call_id()),
                        name="voice-conversation-health",
                    )
                self._end_call_task = asyncio.create_task(
                    self._end_call.wait(), name="voice-end-call"
                )
                if self.settings.idle_prompt_seconds > 0 and self.settings.max_idle_prompts > 0:
                    self._idle_task = asyncio.create_task(
                        self._idle_prompts(), name="voice-idle-prompt"
                    )
                while True:
                    try:
                        audio = await self._read_audio()
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

    async def _read_audio(self):
        monitors = {
            task for task in (self._response_task, self._boundary_failure_task) if task is not None
        }
        end_call = self._end_call_task
        if not monitors and end_call is None:
            return await self.transport.read_audio_chunk()
        reading = asyncio.create_task(self.transport.read_audio_chunk(), name="voice-media-read")
        try:
            waiting = monitors | {reading}
            if end_call is not None:
                waiting.add(end_call)
            done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
            if end_call is not None and end_call in done:
                # A completed farewell closes the media session; teardown sends BYE.
                return None
            for task in monitors & done:
                task.result()
                raise RuntimeError("Conversation response stream ended")
            return reading.result()
        finally:
            if not reading.done():
                reading.cancel()
            await asyncio.gather(reading, return_exceptions=True)

    async def _receive_responses(self):
        async with aclosing(self.conversation_port.responses(self._call_id())) as responses:
            async for speech in responses:
                if self.state != CallState.ACTIVE:
                    return
                # A partial sentence means the same turn is still being written.
                self._awaiting_since = (
                    time.monotonic() if getattr(speech, "is_partial", False) else None
                )
                await self.speak(speech.request_id, speech.text)

    async def _stop_conversation_tasks(self):
        tasks = [
            task
            for task in (
                self._response_task,
                self._boundary_failure_task,
                self._end_call_task,
                self._idle_task,
            )
            if task is not None
        ]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

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
            await self.utterances.submit(
                text, speech_ended_at=getattr(self.stt, "final_speech_at", None)
            )

    async def _deliver_utterance(self, text):
        if self.state != CallState.ACTIVE:
            return
        # Remember whether the caller is closing; a later non-farewell clears it.
        self._caller_farewell = is_farewell(text)
        self._last_prompt_at = time.monotonic()
        # Only a caller utterance means the agent owes an answer; the synthetic
        # idle marker is awaited inline by _idle_prompts itself.
        self._awaiting_since = None if text == IDLE_MARKER else time.monotonic()
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
            # Nothing is pending: do not defer idle nudges for a failed turn.
            self._awaiting_since = None
            self.emit(
                "conversation_port_error",
                operation="final_utterance",
                error_type=type(exc).__name__,
            )
            return
        if authorized_speech is None:
            # Admission only: the answer still arrives through responses(), so
            # the caller must not be treated as idle yet.
            return
        self._awaiting_since = (
            time.monotonic() if getattr(authorized_speech, "is_partial", False) else None
        )
        try:
            delivered = await self.speak(
                authorized_speech.request_id,
                authorized_speech.text,
                speech_ended_at=self.utterances.dispatched_speech_at,
            )
        except Exception as exc:
            self._awaiting_since = None
            self.emit("media_speech_error", error_type=type(exc).__name__)
            return
        if not delivered:
            # Playback did not happen: nothing is owed for this turn.
            self._awaiting_since = None

    async def feed_audio(self, audio):
        validate_pcm(audio)
        if self._output is not None and not self._output.revoked.is_set():
            self._activity_pending = b""
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
        if getattr(self.stt, "uses_speech_classifier", False):
            await self.stt.feed_audio(audio)
            # A second, energy-only clock would let steady noise postpone a
            # classified final forever, even after STT found the end of speech.
            ended_at = self.stt.last_speech_at
            if ended_at is not None and ended_at > self.utterances.last_speech:
                self.utterances.speech(ended_at=ended_at)
            return
        samples = self._activity_pending + audio
        end = len(samples) // PCM.frame_bytes * PCM.frame_bytes
        self._activity_pending = samples[end:]
        received_at = time.monotonic()
        for offset in range(0, end, PCM.frame_bytes):
            frame_end = offset + PCM.frame_bytes
            if rms(samples[offset:frame_end]) >= self.settings.speech_threshold:
                self.utterances.speech(
                    ended_at=received_at - (len(samples) - frame_end) / PCM.bytes_per_second
                )
        await self.stt.feed_audio(audio)

    async def speak(self, request_id, text, *, speech_ended_at=None):
        """Play locally authorized text; keep each output's counters isolated."""
        async with self._speech_lock:
            await self._interrupt_ready.wait()
            if self.state != CallState.ACTIVE:
                raise RuntimeError("Cannot speak outside an active media call")
            if self._retired_outputs:
                raise RuntimeError("Previous provider output is still stopping")
            if self._clear_output_task is not None and not self._clear_output_task.done():
                raise RuntimeError("Previous transport output is still clearing")
            text = text.strip()
            output = _Output(request_id, speech_ended_at=speech_ended_at)
            if not text:
                await self._playback_fact(output, PlaybackState.NOT_STARTED, "empty_text")
                return False
            if len(text) > self.settings.max_tts_chars:
                await self._playback_fact(output, PlaybackState.FAILED, "text_limit")
                raise ValueError("Speech text exceeds configured limit")
            # A segment that is only a greeting/stock intro carries no content:
            # the telephone station already greeted the caller, so speaking it
            # would greet a second time (often the whole reply when the caller
            # says hello first).
            if not has_meaningful_speech(text):
                await self._playback_fact(output, PlaybackState.NOT_STARTED, "stock_opening_only")
                return False
            # A repeated greeting/stock intro is never spoken twice in one call,
            # even when streaming delivers it as separate sentences.
            opening = stock_opening_key(text)
            if opening is not None:
                if opening in self._spoken_openings:
                    await self._playback_fact(
                        output, PlaybackState.NOT_STARTED, "duplicate_opening"
                    )
                    return False
                self._spoken_openings.add(opening)
            self.detector.reset()
            self.detector.echo_rms = 0
            self._output_started = output.started_at
            self._output = output
            self._last_prompt_at = time.monotonic()
            task = asyncio.create_task(self._produce(output, text), name="voice-playback")
            self._output_task = task
            revoked = asyncio.create_task(output.revoked.wait())
            try:
                await asyncio.wait({task, revoked}, return_when=asyncio.FIRST_COMPLETED)
                if output.reason is not None:
                    await self._interrupted_fact(output)
                    return False
                complete = task.result()
                if complete:
                    await self._playback_fact(output, PlaybackState.COMPLETED)
                    if self.settings.hangup_on_farewell and (
                        is_farewell(text) or self._caller_farewell
                    ):
                        self._caller_farewell = False
                        asyncio.create_task(self._graceful_end(), name="voice-farewell-end")
                    return True
                await self._playback_fact(output, PlaybackState.UNKNOWN, "incomplete_transport")
                return False
            except asyncio.CancelledError:
                output.reason = output.reason or "cancelled"
                output.revoked.set()
                await self._cancel_output(task)
                await self._interrupted_fact(output)
                if asyncio.current_task().cancelling():
                    raise
                return False
            except Exception as exc:
                await self._playback_fact(
                    output,
                    PlaybackState.UNKNOWN if output.transport_failed else PlaybackState.FAILED,
                    "transport_" + type(exc).__name__
                    if output.transport_failed
                    else type(exc).__name__,
                )
                raise
            finally:
                output.revoked.set()
                revoked.cancel()
                await asyncio.gather(revoked, return_exceptions=True)
                if not task.done():
                    if not task.cancelling():
                        task.cancel()
                    self._retire_output(task)
                else:
                    observe_task(task)
                self._output_task = self._output = None
                self.detector.reset()
                # The idle clock starts when the agent *stops* speaking, so a
                # short answer is not followed by a nudge a moment later.
                self._last_prompt_at = time.monotonic()
                self._suppress_until = time.monotonic() + (
                    0
                    if output.reason or not output.accepted
                    else self.settings.post_playback_suppression_seconds
                )

    async def _graceful_end(self):
        """Leave the media session shortly after a completed farewell."""
        await asyncio.sleep(self.settings.farewell_grace_seconds)
        if self.state == CallState.ACTIVE:
            self._end_call.set()

    async def _idle_prompts(self):
        """Ask once or twice when the caller has gone quiet.

        The prompt is a synthetic admission, so the reply is authored by
        Conversation like any other turn; the gateway itself never invents speech.
        """
        while self.state == CallState.ACTIVE and self._idle_nudges < self.settings.max_idle_prompts:
            await asyncio.sleep(1.0)
            if self.state != CallState.ACTIVE:
                return
            if self._output is not None or self._awaiting_answer():
                continue
            last = max(self.utterances.last_speech, self._last_prompt_at)
            if time.monotonic() - last < self.settings.idle_prompt_seconds:
                continue
            self._idle_nudges += 1
            await self._deliver_utterance(IDLE_MARKER)
        # Hard silence: the caller never answered the nudges, so the agent ends
        # the call itself instead of leaving the line open forever.
        while self.state == CallState.ACTIVE:
            await asyncio.sleep(0.5)
            if self.state != CallState.ACTIVE:
                return
            if self._output is not None or self._awaiting_answer():
                continue
            last = max(self.utterances.last_speech, self._last_prompt_at)
            if time.monotonic() - last < self.settings.idle_prompt_seconds:
                continue
            self.emit("voice_idle_hangup")
            self._end_call.set()
            return

    def _awaiting_answer(self) -> bool:
        """True while the agent is still preparing a reply to the caller."""
        return (
            self._awaiting_since is not None
            and time.monotonic() - self._awaiting_since < RESPONSE_PENDING_SECONDS
        )

    async def _interrupted_fact(self, output):
        if output.writing:
            state = PlaybackState.UNKNOWN
        elif output.accepted:
            state = (
                PlaybackState.INTERRUPTED if output.reason == "barge_in" else PlaybackState.UNKNOWN
            )
        else:
            state = PlaybackState.NOT_STARTED
        await self._playback_fact(output, state, output.reason)

    async def _produce(self, output, text):
        def active():
            return not output.revoked.is_set() and self.state == CallState.ACTIVE

        async def chunks():
            if hasattr(self.tts, "synthesize_stream"):
                async with aclosing(self.tts.synthesize_stream(text)) as stream:
                    async for chunk in stream:
                        if not active():
                            raise asyncio.CancelledError
                        validate_pcm(chunk)
                        if len(chunk) > 65536:
                            raise ValueError("TTS chunk exceeds configured limit")
                        output.generated += len(chunk)
                        if output.generated > self.settings.max_playback_bytes:
                            raise ValueError("Generated audio exceeds configured limit")
                        yield chunk
            else:
                audio = await self.tts.synthesize(text)
                if not active():
                    raise asyncio.CancelledError
                validate_pcm(audio)
                output.generated = len(audio)
                if output.generated > self.settings.max_playback_bytes:
                    raise ValueError("Generated audio exceeds configured limit")
                for offset in range(0, len(audio), 65536):
                    yield audio[offset : offset + 65536]
            if not output.generated:
                raise ValueError("TTS returned empty audio")

        async def write(chunk):
            if not active():
                raise asyncio.CancelledError
            output.writing = True
            try:
                await self.transport.write_audio_chunk(chunk)
            except Exception:
                output.transport_failed = True
                raise
            if not active():
                raise asyncio.CancelledError
            output.writing = False
            first = output.accepted == 0
            output.accepted += len(chunk)
            if first:
                now = time.monotonic()
                self.emit("media_first_audio", duration_ms=round((now - output.started_at) * 1000))
                if output.speech_ended_at is not None:
                    self.emit(
                        "media_response_gap",
                        duration_ms=round((now - output.speech_ended_at) * 1000),
                    )
            if first or output.accepted - output.last_progress >= 8000:
                output.last_progress = output.accepted
                await self._playback_fact(
                    output,
                    PlaybackState.STARTED if first else PlaybackState.PARTIAL,
                )

        result = await play_pcm_stream(
            chunks(),
            write,
            should_continue=active,
            control=output.control,
            max_bytes=self.settings.max_playback_bytes,
            emit=self.emit,
        )
        return result.complete and active() and output.accepted == output.generated

    async def _playback_fact(self, output, state, reason=None):
        event = PlaybackFact(
            call_id=self._call_id(),
            request_id=output.request_id,
            state=state,
            transport_accepted_bytes=output.accepted,
            generated_bytes=output.generated,
            occurred_at_monotonic_ns=time.monotonic_ns(),
            reason=reason,
        )
        self.emit(
            "voice_playback_fact",
            playback_state=state.value,
            transport_accepted_bytes=output.accepted,
            generated_bytes=output.generated,
        )
        try:
            await self.conversation_port.playback_fact(event)
        except Exception as exc:
            self.emit(
                "conversation_port_error", operation="playback_fact", error_type=type(exc).__name__
            )

    def _retire_output(self, task):
        if task not in self._retired_outputs:
            self._retired_outputs.add(task)
            task.add_done_callback(self._retired_outputs.discard)
            task.add_done_callback(observe_task)

    async def _cancel_output(self, task):
        if task is None or task.done():
            return
        if not task.cancelling():
            task.cancel()
        done, _ = await asyncio.wait({task}, timeout=self.settings.interrupt_timeout)
        if done:
            observe_task(task)
        else:
            self._retire_output(task)
            self.emit("media_output_cancel_timeout")

    async def interrupt(self, *, reason="barge_in"):
        started = time.monotonic()
        self._interrupt_count += 1
        self._interrupt_ready.clear()
        try:
            task, output = self._output_task, self._output
            if output is not None and task is not None and not task.done():
                output.reason = reason
                if reason == "barge_in" and self.settings.smooth_barge_in and output.accepted:
                    output.control.stop()
                    await asyncio.wait({task}, timeout=min(0.06, self.settings.interrupt_timeout))
                output.revoked.set()
                await self._cancel_output(task)
            await self._clear_transport_output()
        finally:
            self._interrupt_count -= 1
            if not self._interrupt_count:
                self._interrupt_ready.set()
            self.emit(
                "media_interrupt_timing", duration_ms=round((time.monotonic() - started) * 1000)
            )

    async def _clear_transport_output(self):
        task = self._clear_output_task
        if task is None or task.done():
            task = asyncio.create_task(self.transport.clear_output(), name="voice-clear-output")
            task.add_done_callback(observe_task)
            self._clear_output_task = task
        try:
            done, _ = await asyncio.wait({task}, timeout=self.settings.interrupt_timeout)
            if done:
                task.result()
            else:
                if not task.cancelling():
                    task.cancel()
                self.emit("media_clear_output_timeout")
        except asyncio.CancelledError:
            if not task.done() and not task.cancelling():
                task.cancel()
            raise

    async def _cleanup(self):
        # Late STT cannot be admitted once the call leaves ACTIVE. Cancel it instead
        # of occupying the SIP/runtime slot while waiting for an unusable result.
        resources = (
            ("output", lambda: self.interrupt(reason="disconnect")),
            ("conversation", self._stop_conversation_tasks),
            ("utterances", self.utterances.close),
            ("transport", self.transport.close),
            ("stt", getattr(self.stt, "abort_stream", self.stt.stop_stream)),
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
