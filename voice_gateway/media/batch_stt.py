"""Bounded batch transcription with VAD and ordered delivery of concurrent results."""

import asyncio
import time
from dataclasses import dataclass
from .interfaces import BaseSTT
from .vad import EnergyVAD, VADSettings
from .completion import bounded_call, observe_task
from .audio import PCM, validate_pcm
from .observability import emit_event


@dataclass(frozen=True)
class BatchSTTSettings:
    concurrency: int = 2
    max_pending: int = 8
    shutdown_timeout: float = 5
    max_utterance_bytes: int = 1000000
    max_final_chars: int = 8000
    early_start_ms: int = 0

    def __post_init__(self):
        if (
            self.concurrency < 1
            or self.max_pending < 1
            or self.shutdown_timeout <= 0
            or self.max_utterance_bytes < 2
            or self.max_final_chars < 1
            or self.early_start_ms < 0
        ):
            raise ValueError("Invalid batch STT settings")


class BatchSTT(BaseSTT):
    """Override _transcribe, _open and _close; callbacks receive turns in input order.

    Backlog overflow fails explicitly and leaves the current utterance buffered.
    Stop drains accepted work up to a deadline, then cancels it and closes the client.
    Abort discards buffered audio and cancels work immediately after a call ends.
    """

    def __init__(
        self, *, vad=VADSettings(), queue=BatchSTTSettings(), emit=None, speech_classifier=None
    ):
        if queue.early_start_ms >= vad.silence_end_ms:
            raise ValueError("Early STT must precede the confirmed speech endpoint")
        self.vad = EnergyVAD(
            vad, max_buffer_bytes=queue.max_utterance_bytes, classifier=speech_classifier
        )
        self.uses_speech_classifier = speech_classifier is not None
        self.queue = queue
        self.emit = lambda event, **values: emit_event(event, observer=emit, **values)
        self._speech_ended_at = self.final_speech_at = None
        self._active_tasks = set()
        self._request_slots = asyncio.Semaphore(queue.concurrency)
        self._last_transcription_task = None
        self._on_partial = self._on_final = self._on_eou = None
        self._accepting = True  # Also supports offline feed/flush with fake transcribe.
        self._deliver_results = True
        self._pending_frame = b""
        self._early_task = self._early_confirmation = None
        self._early_attempted = False

    @property
    def last_speech_at(self):
        return self._speech_ended_at

    async def _open(self):
        pass

    async def _close(self):
        pass

    async def _transcribe(self, pcm):
        raise NotImplementedError

    async def start_stream(self, on_partial=None, on_final=None, on_eou=None):
        if self._active_tasks:
            raise RuntimeError("Cannot restart STT with pending work")
        self._on_partial, self._on_final, self._on_eou = on_partial, on_final, on_eou
        self._accepting = True
        self._deliver_results = True
        self._pending_frame = b""
        self._early_attempted = False
        self.vad.reset()
        self._speech_ended_at = self.final_speech_at = None
        await self._open()

    async def feed_audio(self, chunk):
        if not self._accepting:
            raise RuntimeError("STT stream is stopped")
        validate_pcm(chunk)
        audio = self._pending_frame + chunk
        end = len(audio) // PCM.frame_bytes * PCM.frame_bytes
        self._pending_frame = audio[end:]
        received_at = time.monotonic()
        for offset in range(0, end, PCM.frame_bytes):
            frame_end = offset + PCM.frame_bytes
            await self._feed_frame(
                audio[offset:frame_end],
                received_at - (len(audio) - frame_end) / PCM.bytes_per_second,
            )

    async def _feed_frame(self, frame, ended_at):
        try:
            ready = self.vad.feed(frame)
        except RuntimeError:
            self.emit("stt_acoustic_buffer_rejected", audio_bytes=self.vad.buffer_bytes)
            raise
        if self.vad.has_speech and self.vad.silence_ms == 0:
            self._speech_ended_at = ended_at
            self._discard_early()
        if ready:
            await self._flush_utterance()
        elif (
            self._accepting
            and self.queue.early_start_ms
            and not self._early_attempted
            and self.vad.speech_ms >= self.vad.settings.min_speech_ms
            and self.vad.silence_ms >= self.queue.early_start_ms
            and len(self._active_tasks) < self.queue.max_pending
            and not self._request_slots.locked()
        ):
            # Copy, never consume: resumed speech still needs the entire utterance.
            self._early_attempted = True
            self._early_started_at = time.monotonic()
            self._early_confirmation = asyncio.get_running_loop().create_future()
            self._early_task = self._start_transcription(
                b"".join(self.vad.buffer), confirmation=self._early_confirmation
            )
            self.emit("stt_early_started", silence_ms=round(self.vad.silence_ms))

    def _discard_early(self):
        if self._early_task is None:
            return
        # The decision also fences a provider that suppresses cancellation.
        self._early_confirmation.set_result(False)
        if not self._early_task.done() and not self._early_task.cancelling():
            self._early_task.cancel("Unconfirmed acoustic endpoint discarded")
        self._early_task = self._early_confirmation = None
        self.emit("stt_early_discarded")

    def _reset_buffer(self):
        self._discard_early()
        self._early_attempted = False
        self.vad.reset()
        self._pending_frame = b""

    async def _flush_utterance(self):
        if self._early_task is None and len(self._active_tasks) >= self.queue.max_pending:
            self.emit("stt_backlog_rejected", pending_tasks=len(self._active_tasks))
            raise RuntimeError("Live transcription backlog exceeded; audio remains buffered")
        endpoint = {
            "audio_ms": round(self.vad.buffer_ms),
            "speech_ms": round(self.vad.speech_ms),
            "silence_ms": round(self.vad.silence_ms),
            "endpoint_reason": (
                "silence"
                if self.vad.silence_ms >= self.vad.settings.silence_end_ms
                else "limit"
                if self.vad.buffer_ms >= self.vad.settings.max_utterance_ms
                else "flush"
            ),
        }
        pcm = self.vad.take()
        self._early_attempted = False
        if not pcm:
            self._discard_early()
            return
        if len(pcm) > self.queue.max_utterance_bytes:
            self.emit("stt_utterance_rejected", audio_bytes=len(pcm))
            self._discard_early()
            return
        self.emit("stt_endpoint", **endpoint)
        if self._early_task is not None:
            task = self._early_task
            self._early_confirmation.set_result(True)
            self._early_task = self._early_confirmation = None
            self.emit(
                "stt_early_confirmed",
                duration_ms=round((time.monotonic() - self._early_started_at) * 1000),
            )
        else:
            task = self._start_transcription(pcm)
        self._last_transcription_task = task

    def _start_transcription(self, pcm, *, confirmation=None):
        task = asyncio.create_task(
            self._transcribe_and_callback(
                pcm,
                self._last_transcription_task,
                self._speech_ended_at,
                time.monotonic(),
                confirmation=confirmation,
            )
        )
        self._active_tasks.add(task)
        self.emit("stt_queue_depth", pending_tasks=len(self._active_tasks))
        task.add_done_callback(self._active_tasks.discard)
        return task

    async def _transcribe_and_callback(
        self, pcm, previous=None, speech_ended_at=None, queued_at=None, *, confirmation=None
    ):
        text = ""
        queued_at = time.monotonic() if queued_at is None else queued_at
        try:
            async with self._request_slots:
                started = time.monotonic()
                text = await self._transcribe(pcm)
                if len(text) > self.queue.max_final_chars:
                    self.emit("stt_final_rejected", text_chars=len(text))
                    text = ""
                self.emit(
                    "stt_request_timing",
                    queue_ms=round((started - queued_at) * 1000),
                    duration_ms=round((time.monotonic() - started) * 1000),
                )
                if not text:
                    self.emit("stt_empty_result", audio_ms=len(pcm) * 1000 // 16000)
        except Exception as exc:
            self.emit("stt_transcribe_error", error_type=type(exc).__name__)
        if confirmation is not None and not await asyncio.shield(confirmation):
            return
        if previous is not None:
            try:
                await asyncio.shield(previous)
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling():
                    raise
            except Exception:
                pass
        if text and self._deliver_results and not asyncio.current_task().cancelling():
            try:
                # Transcripts are sensitive communication content; never log them.
                self.emit("stt_final", text_chars=len(text), audio_ms=len(pcm) * 1000 // 16000)
                if self._on_final:
                    self.final_speech_at = speech_ended_at
                    await self._on_final(text)
                if self._on_eou and self._deliver_results:
                    await self._on_eou()
            except Exception as exc:
                self.emit("stt_callback_error", error_type=type(exc).__name__)
            finally:
                self.final_speech_at = None

    async def _settle_tasks(self, tasks, *, timeout, cancellation_grace):
        tasks = {task for task in tasks if not task.done()}
        if not tasks:
            return ()
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        for task in pending:
            if not task.cancelling():
                task.cancel("STT stream stopping")
        if pending:
            cancelled, pending = await asyncio.wait(pending, timeout=cancellation_grace)
            done.update(cancelled)
        if done:
            await asyncio.gather(*done, return_exceptions=True)
        for task in pending:
            task.add_done_callback(observe_task)
        return tuple(pending)

    async def stop_stream(self):
        await self._stop_stream(drain=True)

    async def abort_stream(self):
        await self._stop_stream(drain=False)

    async def _stop_stream(self, *, drain):
        self._accepting = False
        if not drain:
            self._deliver_results = False
        loop = asyncio.get_running_loop()
        timeout = self.queue.shutdown_timeout if drain else min(0.5, self.queue.shutdown_timeout)
        deadline = loop.time() + timeout
        cancellation_grace = min(0.1, timeout / 4)
        close_reserve = min(0.25, timeout / 4)
        drain_timeout = max(0, timeout - cancellation_grace - close_reserve) if drain else 0
        cancellation = None
        failure = None
        drain_finished = False
        try:
            tail, self._pending_frame = self._pending_frame, b""
            if drain and tail:
                await self._feed_frame(tail, time.monotonic())
            if (
                drain
                and self.vad.speech_ms >= self.vad.settings.min_speech_ms
                and (
                    self._early_task is not None or len(self._active_tasks) < self.queue.max_pending
                )
            ):
                await self._flush_utterance()
            tasks = tuple(self._active_tasks)
            if tasks:
                if not drain:
                    for task in tasks:
                        if not task.done():
                            task.cancel("Call ended; transcription no longer usable")
                pending = await self._settle_tasks(
                    tasks, timeout=drain_timeout, cancellation_grace=cancellation_grace
                )
                if pending:
                    self.emit("stt_shutdown_tasks_pending", pending_tasks=len(pending))
            drain_finished = True
        except asyncio.CancelledError as exc:
            cancellation = exc
        except Exception as exc:
            failure = exc
        finally:
            self._deliver_results = False
            tasks = tuple(self._active_tasks)
            if not drain_finished and tasks:
                for task in tasks:
                    if not task.done():
                        task.cancel("STT stream stopping")
                try:
                    pending = await self._settle_tasks(
                        tasks,
                        timeout=0,
                        cancellation_grace=min(
                            cancellation_grace,
                            max(0, deadline - loop.time() - close_reserve),
                        ),
                    )
                except asyncio.CancelledError as exc:
                    cancellation = cancellation or exc
                    pending = tuple(task for task in tasks if not task.done())
                    for task in pending:
                        task.add_done_callback(observe_task)
                if pending:
                    self.emit("stt_shutdown_tasks_pending", pending_tasks=len(pending))
            remaining = max(0, deadline - loop.time())
            try:
                closed = await bounded_call(
                    self._close(),
                    timeout=remaining,
                    cancellation_grace=0,
                )
                if not closed:
                    self.emit("stt_client_close_timeout")
            except asyncio.CancelledError as exc:
                cancellation = cancellation or exc
            except Exception as exc:
                failure = failure or exc
                self.emit("stt_client_close_error", error_type=type(exc).__name__)
            self._last_transcription_task = None
            self._reset_buffer()
            self.emit("stt_stream_stopped")
        if cancellation is not None:
            raise cancellation
        if failure is not None:
            raise failure
