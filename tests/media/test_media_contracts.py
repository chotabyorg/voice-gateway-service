"""Portable media contracts. No real SIP, Asterisk, cloud speech or product state."""

import asyncio
import ast
import io
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import uuid
import wave

import pytest
from tests.media.media_fakes import (
    pcm,
    utterance_fixture,
    FakeSTT,
    FakeTTS,
    FakeWriter,
    socket_frame,
)
from voice_gateway.media.audio import PCM, rms, duration_ms, pcm_to_wav, mix_pcm
from voice_gateway.media.audiosocket import AudioSocketTransport
from voice_gateway.media.interfaces import TransportIdleTimeout
from voice_gateway.media.batch_stt import BatchSTT, BatchSTTSettings
from voice_gateway.media.interruption import InterruptionDetector, InterruptionSettings
from voice_gateway.media.playback import play_pcm
from voice_gateway.media.providers.groq import GroqSTT, GroqSTTSettings
from voice_gateway.media.providers.edge import EdgeTTS, EdgeTTSSettings
from voice_gateway.media.resilience import ResilientSTT, ResilientTTS
from voice_gateway.media.speech import play_speech_stream
from voice_gateway.media.utterances import UtteranceCoordinator
from voice_gateway.media.vad import EnergyVAD, VADSettings


def test_core_dependency_boundary_and_import_without_site_packages():
    root = Path(__file__).resolve().parents[2]
    media = root / "voice_gateway/media"
    for path in media.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and node.level:
                assert node.level <= len(path.relative_to(media).parts), path
                continue
            modules = (
                [node.module]
                if isinstance(node, ast.ImportFrom)
                else [item.name for item in node.names]
                if isinstance(node, ast.Import)
                else []
            )
            for module in modules:
                if path == media / "providers" / "speechkit.py" and module.split(".")[0] in {
                    "grpc",
                    "yandex",
                }:
                    continue  # Optional SpeechKit SDK, imported only at explicit activation.
                assert module.split(".")[0] in sys.stdlib_module_names | {
                    "voice_gateway",
                    "groq",
                    "httpx",
                    "edge_tts",
                    "fastapi",
                    "pydantic",
                    "pydantic_settings",
                    "uvicorn",
                }, (path, module)
    code = """
import importlib, pkgutil, sys
import voice_gateway.media
for module in pkgutil.walk_packages(voice_gateway.media.__path__, 'voice_gateway.media.'):
    importlib.import_module(module.name)
assert not any(x.startswith(('voice_agent', 'sqlite3', 'groq', 'edge_tts')) for x in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, "-S", "-c", code], cwd=root, capture_output=True, timeout=15
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")


def test_canonical_audio_roundtrip_and_saturated_mix():
    audio = pcm(-1200, 40)
    assert PCM.frame_bytes == 320 and rms(audio) == 1200 and duration_ms(audio) == 40
    with wave.open(io.BytesIO(pcm_to_wav(audio))) as output:
        assert output.getparams()[:3] == (1, 2, 8000)
        assert output.readframes(output.getnframes()) == audio
    assert mix_pcm(pcm(30000), pcm(30000)) == pcm(32767)
    assert mix_pcm(pcm(-30000), pcm(-30000)) == pcm(-32768)
    assert mix_pcm(pcm(1000), b"") == pcm(1000)


@pytest.mark.parametrize("bad", [b"\1", bytearray(b"\1\0"), "audio"])
def test_audio_rejects_incomplete_or_untyped_samples(bad):
    with pytest.raises(ValueError):
        rms(bad)


def test_vad_preroll_pause_click_and_max_utterance():
    vad = EnergyVAD()
    frames = utterance_fixture()
    assert [vad.feed(frame) for frame in frames] == [False, False, False, False, True]
    assert vad.take() == b"".join(frames)
    vad.feed(pcm(0, 5000))
    assert len(vad.preroll) == 200 * 16
    assert not vad.feed(pcm(2000))
    assert not vad.feed(pcm(0, 500)) and not vad.buffer
    bounded = EnergyVAD(VADSettings(max_utterance_ms=100))
    assert [bounded.feed(pcm()) for _ in range(5)] == [False] * 4 + [True]
    assert len(bounded.take()) == 1600


def test_interruption_keeps_quiet_leadin_and_rejects_click():
    detector = InterruptionDetector(InterruptionSettings(required_ms=100))
    assert detector.feed(pcm(100)) is None
    assert detector.feed(pcm(3000)) is None
    assert detector.feed(pcm(0)) is None  # Reset sustained speech after a click.
    loud = [pcm(1000) for _ in range(5)]
    events = [detector.feed(frame) for frame in loud]
    assert all(event is None for event in events[:-1])
    assert events[-1].chunks[0] == pcm(100)
    assert events[-1].chunks[-5:] == tuple(loud)
    assert detector.candidate_ms == 0
    for _ in range(100):
        detector.feed(pcm(0))
    assert sum(map(len, detector.chunks)) <= 320 * 15


def test_guarded_interruption_calibrates_echo_and_waits_for_real_speech():
    detector = InterruptionDetector(InterruptionSettings(guarded=True, required_ms=40))
    assert detector.feed(pcm(1000), elapsed=0.1) is None
    assert detector.feed(pcm(3000), elapsed=0.25) is None
    assert detector.feed(pcm(1200), elapsed=0.4) is None
    assert detector.feed(pcm(2000), elapsed=0.5) is None
    event = detector.feed(pcm(2000), elapsed=0.52)
    assert event.threshold == 1350 and event.chunks == (pcm(2000), pcm(2000))


@pytest.mark.asyncio
async def test_playback_pacing_partial_frame_and_interruption():
    now, sleeps, sent = [0.0], [], []

    async def write(chunk):
        sent.append(chunk)
        now[0] += 0.004

    async def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    audio = pcm(2000, 55)
    result = await play_pcm(audio, write, clock=lambda: now[0], sleep=sleep)
    assert result.complete and result.sent_bytes == 880
    assert now[0] == pytest.approx(0.055) and b"".join(sent) == audio
    result = await play_pcm(
        pcm(1, 100), write, should_continue=lambda: len(sent) < 4, clock=lambda: now[0], sleep=sleep
    )
    assert not result.complete and result.sent_bytes == 320


@pytest.mark.asyncio
async def test_playback_does_not_burst_after_io_stall():
    now, writes = [0.0], []

    async def write(chunk):
        writes.append(now[0])
        if len(writes) == 1:
            now[0] += 0.1

    async def sleep(seconds):
        now[0] += seconds

    await play_pcm(pcm(1, 60), write, clock=lambda: now[0], sleep=sleep)
    # The first write finishes at 0.1; leave one frame interval before the next.
    assert writes == pytest.approx([0, 0.12, 0.14])


@pytest.mark.asyncio
async def test_audiosocket_handshake_fragmented_frames_dtmf_and_hangup():
    reader, writer = asyncio.StreamReader(), FakeWriter()
    identifier = uuid.uuid4()
    raw = (
        socket_frame(1, identifier.bytes)
        + socket_frame(3, b"1")
        + socket_frame(0x10, pcm())
        + socket_frame(0)
    )
    transport = AudioSocketTransport(reader, writer)

    async def feed():
        for offset in range(0, len(raw), 7):
            reader.feed_data(raw[offset : offset + 7])
            await asyncio.sleep(0)
        reader.feed_eof()

    task = asyncio.create_task(feed())
    try:
        await transport.prepare()
        await transport.prepare()  # Does not consume the next frame.
        assert transport.call_id == str(identifier)
        assert await transport.read_audio_chunk() == pcm()
        await transport.write_audio_chunk(pcm(2000))
        assert writer.frames == [socket_frame(0x10, pcm(2000))]
        assert await transport.read_audio_chunk() is None
    finally:
        await transport.close()
        await transport.close()
        await task
    assert writer.closed


@pytest.mark.asyncio
async def test_audiosocket_idle_timeout_releases_a_stalled_session():
    reader, writer = asyncio.StreamReader(), FakeWriter()
    reader.feed_data(socket_frame(1, uuid.uuid4().bytes))
    transport = AudioSocketTransport(reader, writer, idle_timeout=0.01)
    await transport.prepare()
    with pytest.raises(TransportIdleTimeout, match="idle timeout"):
        await asyncio.wait_for(transport.read_audio_chunk(), timeout=0.1)
    await transport.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "wire,expected",
    [(b"\x10\x01", None), (b"\x10\x00\x04\0", None)],
)
async def test_audiosocket_truncated_eof(wire, expected):
    reader = asyncio.StreamReader()
    reader.feed_data(wire)
    reader.feed_eof()
    transport = AudioSocketTransport(reader, FakeWriter())
    assert await transport.read_audio_chunk() is expected
    await transport.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("wire", [socket_frame(1, b"bad"), socket_frame(0x10, pcm())])
async def test_audiosocket_rejects_bad_handshake(wire):
    reader = asyncio.StreamReader()
    reader.feed_data(wire)
    reader.feed_eof()
    transport = AudioSocketTransport(reader, FakeWriter())
    with pytest.raises(ValueError, match="UUID"):
        await transport.prepare()
    await transport.close()


@pytest.mark.asyncio
async def test_audiosocket_validates_pcm_rate_and_closes_after_failed_write():
    reader, writer = asyncio.StreamReader(), FakeWriter()
    reader.feed_data(socket_frame(0x12, pcm()))
    transport = AudioSocketTransport(reader, writer)
    with pytest.raises(ValueError, match="sample rate"):
        await transport.read_audio_chunk()
    with pytest.raises(ValueError):
        await transport.write_audio_chunk(b"odd")
    writer.fail_write = True
    with pytest.raises(ConnectionError):
        await transport.write_audio_chunk(pcm())
    await transport.close()
    assert writer.closed


@pytest.mark.asyncio
async def test_batch_stt_concurrency_order_failure_and_backlog():
    queue = BatchSTT(queue=BatchSTTSettings(concurrency=2, max_pending=3))
    release, two_active = asyncio.Event(), asyncio.Event()
    active, peak, delivered, events = 0, 0, [], []
    queue.emit = lambda event, **values: events.append(event)

    async def transcribe(audio):
        nonlocal active, peak
        active += 1
        peak = max(active, peak)
        if active == 2:
            two_active.set()
        try:
            if audio == pcm(1):
                await release.wait()
                raise PermissionError("Provider rejected request")
            return str(audio[0])
        finally:
            active -= 1

    async def final(text):
        delivered.append(text)

    queue._transcribe = transcribe
    await queue.start_stream(on_final=final)
    for value in (1, 2, 3):
        queue.vad.buffer = [pcm(value)]
        await queue._flush_utterance()
    queue.vad.buffer = [pcm(4)]
    with pytest.raises(RuntimeError, match="backlog"):
        await queue._flush_utterance()
    assert queue.vad.buffer == [pcm(4)]
    await asyncio.wait_for(two_active.wait(), 1)
    assert delivered == []
    release.set()
    await asyncio.gather(*queue._active_tasks)
    await queue.stop_stream()
    assert peak == 2 and delivered == ["2", "3"] and "stt_transcribe_error" in events


@pytest.mark.asyncio
async def test_batch_stop_cancels_blocked_request_and_closes_client():
    queue = BatchSTT(queue=BatchSTTSettings(shutdown_timeout=0.01))
    closed = []

    async def transcribe(audio):
        await asyncio.Event().wait()

    async def close():
        closed.append(True)

    queue._transcribe, queue._close = transcribe, close
    queue.vad.buffer = [pcm()]
    await queue._flush_utterance()
    await queue.stop_stream()
    assert not queue._active_tasks and closed == [True]
    with pytest.raises(RuntimeError, match="stopped"):
        await queue.feed_audio(pcm())


@pytest.mark.asyncio
async def test_batch_stop_bounds_cancellation_resistant_request_and_closes_client():
    entered, release, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    events = []
    queue = BatchSTT(
        queue=BatchSTTSettings(shutdown_timeout=0.08),
        emit=lambda event, **values: events.append((event, values)),
    )

    async def transcribe(_audio):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()
        return "late result"

    async def close():
        closed.set()

    queue._transcribe, queue._close = transcribe, close
    queue.vad.buffer = [pcm()]
    await queue._flush_utterance()
    task = next(iter(queue._active_tasks))
    await entered.wait()
    started = asyncio.get_running_loop().time()
    await asyncio.wait_for(queue.stop_stream(), timeout=0.2)
    assert asyncio.get_running_loop().time() - started < 0.15
    assert closed.is_set() and not task.done()
    assert ("stt_shutdown_tasks_pending", {"pending_tasks": 1}) in events
    release.set()
    await asyncio.wait_for(task, timeout=1)


@pytest.mark.asyncio
async def test_groq_adapter_request_wav_language_and_client_cleanup():
    requests, usage, closed, texts, kwargs = [], [], [], [], []

    async def create(**options):
        requests.append(options)
        return SimpleNamespace(text=" hello ")

    async def close():
        closed.append(True)

    client = SimpleNamespace(
        audio=SimpleNamespace(transcriptions=SimpleNamespace(create=create)), close=close
    )

    def factory(**options):
        kwargs.append(options)
        return client

    async def final(text):
        texts.append(text)

    adapter = GroqSTT(
        GroqSTTSettings("fake-key", language="ru"),
        client_factory=factory,
        on_audio_usage=usage.append,
    )
    assert "fake-key" not in repr(adapter.settings)
    await adapter.start_stream(on_final=final)
    for frame in utterance_fixture():
        await adapter.feed_audio(frame)
    await adapter.stop_stream()
    assert texts == ["hello"] and closed == [True] and usage == [pytest.approx(0.9)]
    assert requests[0]["language"] == "ru" and kwargs[0]["timeout"] == 30
    with wave.open(io.BytesIO(requests[0]["file"][1])) as output:
        assert output.readframes(output.getnframes()) == b"".join(utterance_fixture())


@pytest.mark.asyncio
async def test_edge_adapter_explicit_voice_mp3_decoder_and_pcm_validation():
    clients, decoded = [], []

    class Client:
        def __init__(self, text, **settings):
            clients.append((text, settings))

        async def stream(self):
            yield {"type": "metadata", "data": b"ignored"}
            yield {"type": "audio", "data": b"mp3-a"}
            yield {"type": "audio", "data": b"-b"}

    async def decoder(audio, **kwargs):
        decoded.append(audio)
        return pcm()

    adapter = EdgeTTS(
        EdgeTTSSettings(voice="test-voice"), communicate_factory=Client, decoder=decoder
    )
    assert await adapter.synthesize("  hello  ") == pcm()
    assert clients[0] == ("hello", {"voice": "test-voice", "rate": "+0%", "pitch": "+0Hz"})
    assert decoded == [b"mp3-a-b"] and await adapter.synthesize("") == b""

    async def malformed(audio, **kwargs):
        return b"odd"

    adapter.decoder = malformed
    with pytest.raises(ValueError):
        await adapter.synthesize("bad")
    await adapter.close()


@pytest.mark.asyncio
async def test_edge_adapter_bounds_provider_wait_and_closes_stream():
    closed = asyncio.Event()

    class StallingClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def stream(self):
            try:
                await asyncio.Event().wait()
                yield {"type": "audio", "data": b"unreachable"}
            finally:
                closed.set()

    adapter = EdgeTTS(EdgeTTSSettings(stream_timeout=0.01), communicate_factory=StallingClient)
    with pytest.raises(TimeoutError):
        await adapter.synthesize("bounded request")
    assert closed.is_set()


@pytest.mark.asyncio
async def test_fallback_adapters_resource_ownership_and_validation():
    failed, healthy = FakeSTT(), FakeSTT()

    async def fail(**kwargs):
        raise ConnectionError()

    failed.start_stream = fail
    stt = ResilientSTT(failed, healthy)
    await stt.start_stream()
    await stt.feed_audio(pcm())
    await stt.stop_stream()
    assert failed.stopped and healthy.stopped and healthy.chunks == [pcm()]
    first, second, usage = FakeTTS(b"bad"), FakeTTS(), []
    tts = ResilientTTS(first, second, on_usage=usage.append)
    assert await tts.synthesize("hello") == second.audio
    assert usage == [5, 5]
    await tts.close()
    assert first.closed and second.closed


@pytest.mark.asyncio
async def test_turn_coordinator_continuation_and_shutdown():
    delivered = []

    async def callback(text):
        delivered.append(text)

    turns = UtteranceCoordinator(callback, silence_seconds=0.01, continuation_seconds=0.03)
    turns.speech()
    await turns.submit("I want")
    await asyncio.sleep(0.015)
    assert delivered == []
    await turns.submit("a test call")
    await asyncio.wait_for(turns.task, 1)
    assert delivered == ["I want a test call"]
    await turns.submit("and")
    await turns.close()
    assert turns.closed and not turns.pending


@pytest.mark.asyncio
async def test_portable_speech_prefetch_cancellation_releases_producer_and_tts():
    pending, cancelled, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def segments():
        try:
            yield "First"
            yield "Second"
            await asyncio.Event().wait()
        finally:
            closed.set()

    async def synthesize(text):
        if text == "Second":
            pending.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        return pcm()

    async def play(audio):
        await asyncio.Event().wait()

    task = asyncio.create_task(
        play_speech_stream(
            segments(),
            synthesize=synthesize,
            play=play,
            guard=lambda text: (text, False),
            on_spoken=lambda text: None,
        )
    )
    await asyncio.wait_for(pending.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set() and closed.is_set()
