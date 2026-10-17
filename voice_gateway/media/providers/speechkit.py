"""SpeechKit v3 streaming on telephone PCM; local VAD owns utterance boundaries.

One TLS channel per call, one bounded RPC per acoustic utterance. Audio is sent
while the caller speaks; only locally confirmed finals reach Conversation.
No partial transcript, provider EOU, retry or alternate provider grants a turn.
"""

import asyncio
import time
from contextlib import aclosing
from dataclasses import dataclass, field

from ..audio import PCM, validate_pcm
from ..completion import bounded_call, deadline_timeout, finish_owned_task, observe_task
from ..interfaces import BaseSTT, BaseTTS
from ..speech_text import pronounce_byn
from ..observability import emit_event
from ..vad import EnergyVAD, VADSettings


@dataclass(frozen=True)
class SpeechKitSettings:
    api_key: str = field(repr=False)
    language: str = "ru-RU"
    model: str = "general"
    final_timeout: float = 5
    max_pending: int = 4
    # Queued 20 ms frames per utterance. Sized beyond a full 30 s utterance so
    # continuous speech never fails on a transiently slow stream; a genuinely
    # stuck RPC still fails through its own timeouts with a proper error event.
    max_audio_queue_frames: int = 1600
    max_final_chars: int = 8000
    shutdown_timeout: float = 2

    def __post_init__(self):
        if (
            not self.api_key
            or not self.api_key.isascii()
            or self.language not in {"ru-RU", "en-US"}
            or self.model != "general"
            or not 0 < self.final_timeout <= 15
            or not 1 <= self.max_pending <= 8
            or not 32 <= self.max_audio_queue_frames <= 4096
            or not 1 <= self.max_final_chars <= 8000
            or not 0 < self.shutdown_timeout <= 5
        ):
            raise ValueError("Invalid SpeechKit settings")


@dataclass
class _Utterance:
    audio: asyncio.Queue
    confirmed: bool = False
    endpoint_at: float = 0
    speech_at: float | None = None
    task: asyncio.Task | None = None


def _connection():
    import grpc
    from yandex.cloud.ai.stt.v3.stt_service_pb2_grpc import RecognizerStub

    channel = grpc.aio.secure_channel(
        "stt.api.cloud.yandex.net:443",
        grpc.ssl_channel_credentials(),
        options=(
            ("grpc.enable_retries", 0),
            ("grpc.max_receive_message_length", 65536),
            ("grpc.max_send_message_length", 65536),
        ),
    )
    return channel, RecognizerStub(channel)


class SpeechKitSTT(BaseSTT):
    def __init__(
        self,
        settings,
        *,
        vad=VADSettings(),
        speech_classifier=None,
        connection_factory=_connection,
        emit=None,
    ):
        self.settings = settings
        self.vad = EnergyVAD(vad, max_buffer_bytes=1_000_000, classifier=speech_classifier)
        self.uses_speech_classifier = speech_classifier is not None
        self.connection_factory = connection_factory
        self.emit = lambda event, **values: emit_event(event, observer=emit, **values)
        self.last_speech_at = self.final_speech_at = None
        self._accepting = self._deliver = False
        self._channel = self._stub = None
        self._current = self._previous = None
        self._tasks = set()
        self._rpcs = set()
        self._error = False
        self._pending_frame = b""
        self._pending_count = 0

    async def start_stream(self, on_partial=None, on_final=None, on_eou=None):
        if self._channel is not None or self._tasks:
            raise RuntimeError("SpeechKit stream already started")
        self._on_final, self._on_eou = on_final, on_eou
        self._channel, self._stub = self.connection_factory()
        self._accepting = self._deliver = True
        self._error = False
        self._pending_count = 0
        self.last_speech_at = self.final_speech_at = None
        self._pending_frame = b""
        self.vad.reset()

    def _own(self, task):
        self._tasks.add(task)

        def done(completed):
            self._tasks.discard(completed)
            if not completed.cancelled() and completed.exception() is not None:
                self._error = True
                self.emit("stt_transcribe_error", error_type="SpeechKitStreamError")

        task.add_done_callback(done)
        return task

    def _enqueue(self, utterance, chunk):
        try:
            utterance.audio.put_nowait(chunk)
        except asyncio.QueueFull:
            self.emit(
                "stt_audio_backlog",
                frames=utterance.audio.qsize(),
                limit=self.settings.max_audio_queue_frames,
            )
            raise RuntimeError("SpeechKit audio backlog exceeded") from None

    async def feed_audio(self, chunk):
        if not self._accepting or self._error:
            raise RuntimeError("SpeechKit stream unavailable")
        validate_pcm(chunk)
        if len(chunk) > 65536:
            raise ValueError("SpeechKit input chunk exceeds limit")
        audio = self._pending_frame + chunk
        end = len(audio) // PCM.frame_bytes * PCM.frame_bytes
        self._pending_frame = audio[end:]
        received_at = time.monotonic()
        for offset in range(0, end, PCM.frame_bytes):
            if not self._accepting or self._error:
                raise RuntimeError("SpeechKit stream unavailable")
            frame_end = offset + PCM.frame_bytes
            frame = audio[offset:frame_end]
            ready = self.vad.feed(frame)
            if self.vad.has_speech and self.vad.silence_ms == 0:
                self.last_speech_at = received_at - (len(audio) - frame_end) / PCM.bytes_per_second
            if self._current is None and self.vad.speech_ms >= self.vad.settings.min_speech_ms:
                if self._pending_count >= self.settings.max_pending:
                    raise RuntimeError("SpeechKit utterance backlog exceeded")
                self._pending_count += 1
                utterance = self._current = _Utterance(
                    asyncio.Queue(self.settings.max_audio_queue_frames)
                )
                # Include bounded preroll once; subsequent frames are never replayed.
                for buffered in self.vad.buffer:
                    for n in range(0, len(buffered), PCM.frame_bytes):
                        self._enqueue(utterance, buffered[n : n + PCM.frame_bytes])
                utterance.task = self._own(asyncio.create_task(self._recognize(utterance)))
                self.emit("stt_stream_started")
            elif self._current is not None:
                self._enqueue(self._current, frame)
            if ready:
                self._finish_utterance()
            # feed_audio can receive multiple accumulated RTP frames.
            await asyncio.sleep(0)

    def _finish_utterance(self):
        utterance, self._current = self._current, None
        if utterance is not None:
            utterance.confirmed = True
            utterance.endpoint_at = time.monotonic()
            utterance.speech_at = self.last_speech_at
            self._enqueue(utterance, None)
            previous = self._previous
            self._previous = self._own(asyncio.create_task(self._finish(utterance, previous)))
            self.emit(
                "stt_endpoint",
                audio_ms=round(self.vad.buffer_ms),
                speech_ms=round(self.vad.speech_ms),
                silence_ms=round(self.vad.silence_ms),
                endpoint_reason="silence"
                if self.vad.silence_ms >= self.vad.settings.silence_end_ms
                else "limit",
            )
        self.vad.reset()

    async def _requests(self, utterance):
        from yandex.cloud.ai.stt.v3 import stt_pb2 as p

        yield p.StreamingRequest(
            session_options=p.StreamingOptions(
                recognition_model=p.RecognitionModelOptions(
                    model=self.settings.model,
                    audio_format=p.AudioFormatOptions(
                        raw_audio=p.RawAudio(
                            audio_encoding=p.RawAudio.LINEAR16_PCM,
                            sample_rate_hertz=8000,
                            audio_channel_count=1,
                        )
                    ),
                    text_normalization=p.TextNormalizationOptions(
                        text_normalization=p.TextNormalizationOptions.TEXT_NORMALIZATION_DISABLED,
                        profanity_filter=False,
                        literature_text=False,
                    ),
                    language_restriction=p.LanguageRestrictionOptions(
                        restriction_type=p.LanguageRestrictionOptions.WHITELIST,
                        language_code=[self.settings.language],
                    ),
                    audio_processing_type=p.RecognitionModelOptions.REAL_TIME,
                ),
                eou_classifier=p.EouClassifierOptions(
                    external_classifier=p.ExternalEouClassifier()
                ),
            )
        )
        while True:
            chunk = await utterance.audio.get()
            if chunk is None:
                yield p.StreamingRequest(eou=p.Eou())
                return
            yield p.StreamingRequest(chunk=p.AudioChunk(data=chunk))

    async def _recognize(self, utterance):
        requests = self._requests(utterance)
        rpc = self._stub.RecognizeStreaming(
            metadata=(
                ("authorization", "Api-Key " + self.settings.api_key),
                ("x-data-logging-enabled", "false"),
            ),
            timeout=self.vad.settings.max_utterance_ms / 1000 + self.settings.final_timeout,
        )
        self._rpcs.add(rpc)

        async def send():
            try:
                async for request in requests:
                    await rpc.write(request)
                await rpc.done_writing()
            except BaseException:
                rpc.cancel()
                raise

        sender = asyncio.create_task(send())
        finals, refined, total = [], {}, 0
        try:
            async for response in rpc:
                event = response.WhichOneof("Event")
                if event == "final":
                    alternatives = response.final.alternatives
                    text = alternatives[0].text.strip() if alternatives else ""
                    total += len(text) + 1
                    if total > self.settings.max_final_chars or len(finals) >= 64:
                        raise ValueError("SpeechKit transcript exceeds limit")
                    # Keep every segment so server final_index values stay aligned.
                    finals.append(text)
                elif event == "final_refinement":
                    # Normalized replacement for a previously sent final segment.
                    refinement = response.final_refinement
                    index = refinement.final_index
                    alternatives = refinement.normalized_text.alternatives
                    text = alternatives[0].text.strip() if alternatives else ""
                    total += len(text) + 1
                    if total > self.settings.max_final_chars or not 0 <= index < 64:
                        raise ValueError("SpeechKit refinement exceeds limit")
                    if text:
                        refined[index] = text
                elif event == "partial":
                    # Hypotheses may change. Never hand them to Conversation or log them.
                    if any(
                        len(a.text) > self.settings.max_final_chars
                        for a in response.partial.alternatives
                    ):
                        raise ValueError("SpeechKit partial exceeds limit")
                elif event is None or event in {
                    "eou_update",
                    "status_code",
                    "session_uuid",
                    "audio_cursors",
                    "classifier_update",
                    "speaker_analysis",
                    "conversation_analysis",
                    "summarization",
                    "channel_tag",
                }:
                    # Informational server messages carry no transcript; ignore them.
                    continue
                else:
                    raise ValueError("Unexpected SpeechKit response")
            if not utterance.confirmed:
                raise RuntimeError("SpeechKit closed before local speech endpoint")
            await sender
            segments = [refined.get(index, text) for index, text in enumerate(finals)]
            return " ".join(segment for segment in segments if segment)
        except asyncio.CancelledError:
            if not asyncio.current_task().cancelling():
                raise RuntimeError("SpeechKit stream cancelled by provider") from None
            raise
        finally:
            rpc.cancel()
            self._rpcs.discard(rpc)
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)
            await requests.aclose()
            while not utterance.audio.empty():
                utterance.audio.get_nowait()

    async def _finish(self, utterance, previous):
        try:
            await self._deliver_final(utterance, previous)
        finally:
            self._pending_count -= 1

    async def _deliver_final(self, utterance, previous):
        async with asyncio.timeout(self.settings.final_timeout):
            text = await utterance.task
        self.emit(
            "stt_request_timing",
            queue_ms=0,
            duration_ms=round((time.monotonic() - utterance.endpoint_at) * 1000),
        )
        if previous is not None:
            await asyncio.shield(previous)
        if self._deliver and not asyncio.current_task().cancelling():
            if text:
                self.emit("stt_final", text_chars=len(text))
                self.final_speech_at = utterance.speech_at
                if self._on_final:
                    await self._on_final(text)
                if self._on_eou and self._deliver:
                    await self._on_eou()
            else:
                self.emit("stt_empty_result")

    async def stop_stream(self):
        # Stop is an ended call, never a new acoustic endpoint.
        await self.abort_stream()

    async def abort_stream(self):
        self._accepting = self._deliver = False
        self._pending_frame = b""
        self._current = self._previous = None
        self.vad.reset()
        for rpc in tuple(self._rpcs):
            rpc.cancel()
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        channel, self._channel = self._channel, None

        async def cleanup():
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                if channel is not None:
                    await channel.close(grace=None)

        cleanup_task = asyncio.create_task(
            bounded_call(cleanup(), timeout=self.settings.shutdown_timeout)
        )
        cleanup_task.add_done_callback(observe_task)
        await finish_owned_task(cleanup_task)


@dataclass(frozen=True)
class SpeechKitTTSSettings:
    api_key: str = field(repr=False)
    model: str = "livetts"
    voice: str = "sofia"
    role: str = "support"
    expand_byn: bool = False
    max_text_chars: int = 2000
    max_pcm_bytes: int = 1_920_000
    stream_timeout: float = 120

    def __post_init__(self):
        voices = {
            "livetts": {"sofia": {"support", "casual"}, "vera": {"support", "casual"}},
            "general": {"marina": {"neutral", "friendly", "whisper"}},
        }
        if (
            not self.api_key
            or not self.api_key.isascii()
            or self.role not in voices.get(self.model, {}).get(self.voice, set())
            or not 1 <= self.max_text_chars <= 4000
            or not 2 <= self.max_pcm_bytes <= 8 * 1024 * 1024
            or not 0 < self.stream_timeout <= 600
        ):
            raise ValueError("Invalid SpeechKit TTS settings")


def _tts_connection():
    import grpc
    from yandex.cloud.ai.tts.v3.tts_service_pb2_grpc import SynthesizerStub

    channel = grpc.aio.secure_channel(
        "tts.api.cloud.yandex.net:443",
        grpc.ssl_channel_credentials(),
        options=(
            ("grpc.enable_retries", 0),
            ("grpc.max_receive_message_length", 262144),
            ("grpc.max_send_message_length", 16384),
        ),
    )
    return channel, SynthesizerStub(channel)


class SpeechKitTTS(BaseTTS):
    """Stream authorized speech directly as PCM16 mono 8 kHz.

    This is a speech provider only: it does not stream LLM drafts or grant
    playback authority. LiveTTS has separate text/audio messages; word timings
    are neither assumed nor used as evidence that the caller heard speech.
    """

    def __init__(self, settings, *, connection_factory=_tts_connection):
        self.settings = settings
        self.connection_factory = connection_factory
        self._channel = self._stub = None
        self._rpcs = set()
        self._closed = False
        self._warming = None

    def prewarm(self):
        # Open TLS/HTTP2 before the first answer; a failure only loses the head start.
        if self._closed or self._channel is not None:
            return
        self._channel, self._stub = self.connection_factory()
        ready = getattr(self._channel, "channel_ready", None)
        if ready is not None:
            self._warming = asyncio.create_task(asyncio.wait_for(ready(), 5))
            self._warming.add_done_callback(observe_task)

    async def synthesize(self, text):
        chunks = bytearray()
        async with aclosing(self.synthesize_stream(text)) as source:
            async for chunk in source:
                chunks.extend(chunk)
        return bytes(chunks)

    async def synthesize_stream(self, text):
        from yandex.cloud.ai.tts.v3 import tts_pb2 as p

        if self._closed or self._rpcs:
            raise RuntimeError("SpeechKit TTS unavailable")
        if not isinstance(text, str):
            raise ValueError("Invalid SpeechKit TTS text")
        text = pronounce_byn(text) if self.settings.expand_byn else text
        if (
            not text.strip()
            or len(text) > self.settings.max_text_chars
            or len(text.encode()) > 5000
        ):
            raise ValueError("SpeechKit TTS text exceeds limit or is empty")
        if self._channel is None:
            self._channel, self._stub = self.connection_factory()
        deadline = asyncio.get_running_loop().time() + self.settings.stream_timeout
        rpc = self._stub.StreamSynthesis(
            metadata=(
                ("authorization", "Api-Key " + self.settings.api_key),
                ("x-data-logging-enabled", "false"),
            ),
            timeout=self.settings.stream_timeout,
        )
        self._rpcs.add(rpc)

        async def send():
            try:
                await rpc.write(
                    p.StreamSynthesisRequest(
                        options=p.SynthesisOptions(
                            model=self.settings.model,
                            voice=self.settings.voice,
                            role=self.settings.role,
                            output_audio_spec=p.AudioFormatOptions(
                                raw_audio=p.RawAudio(
                                    audio_encoding=p.RawAudio.LINEAR16_PCM,
                                    sample_rate_hertz=8000,
                                )
                            ),
                            loudness_normalization_type=p.LUFS,
                        )
                    )
                )
                await rpc.write(
                    p.StreamSynthesisRequest(synthesis_input=p.SynthesisInput(text=text))
                )
                await rpc.write(p.StreamSynthesisRequest(force_synthesis=p.ForceSynthesisEvent()))
                await rpc.done_writing()
            except BaseException:
                rpc.cancel()
                raise

        sender = asyncio.create_task(send())
        total, pending = 0, b""
        try:
            source = rpc.__aiter__()
            while True:
                async with deadline_timeout(deadline):
                    try:
                        response = await anext(source)
                    except StopAsyncIteration:
                        break
                if response.chunk_type not in {0, 1, 2, 3, 4}:
                    raise ValueError("Unknown SpeechKit TTS chunk type")
                data = response.audio_chunk.data
                if data and response.chunk_type in {2, 4}:
                    raise ValueError("Unexpected SpeechKit audio in text response")
                total += len(data)
                if total > self.settings.max_pcm_bytes:
                    raise ValueError("SpeechKit TTS audio exceeds limit")
                data = pending + data
                size = len(data) // 2 * 2
                pending = data[size:]
                for offset in range(0, size, 65536):
                    yield data[offset : min(offset + 65536, size)]
            async with deadline_timeout(deadline):
                await sender
            if pending or not total:
                raise ValueError("SpeechKit TTS returned incomplete or empty audio")
        except asyncio.CancelledError:
            if not asyncio.current_task().cancelling():
                raise RuntimeError("SpeechKit TTS provider cancelled") from None
            raise
        except Exception:
            # gRPC error details can contain credentials or submitted text.
            raise RuntimeError("SpeechKit TTS stream failed") from None
        finally:
            rpc.cancel()
            self._rpcs.discard(rpc)
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)

    async def close(self):
        self._closed = True
        if self._warming is not None:
            self._warming.cancel()
            await asyncio.gather(self._warming, return_exceptions=True)
        for rpc in tuple(self._rpcs):
            rpc.cancel()
        channel, self._channel = self._channel, None
        if channel is not None:
            cleanup = asyncio.create_task(bounded_call(channel.close(), timeout=2))
            cleanup.add_done_callback(observe_task)
            await finish_owned_task(cleanup)
