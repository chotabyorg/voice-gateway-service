"""Native loopback gRPC contract: streaming, local EOU, ordering and cleanup."""

import asyncio
from types import SimpleNamespace

import grpc
import pytest
from yandex.cloud.ai.stt.v3 import stt_pb2 as p
from yandex.cloud.ai.stt.v3 import stt_service_pb2_grpc as service

from tests.media.media_fakes import pcm
from voice_gateway.media.providers.speechkit import (
    SpeechKitSettings,
    SpeechKitSTT,
    _Utterance,
)
from voice_gateway.media.vad import VADSettings
from voice_gateway.media.providers import speechkit


class Recognizer(service.RecognizerServicer):
    def __init__(self):
        self.audio = asyncio.Event()
        self.closed = asyncio.Event()
        self.requests = []
        self.metadata = []
        self.active = 0
        self.delay_first = 0
        self.fail = False
        self.hang = False
        self.refinement = None
        self.refinement_index = 0
        self.informational = False

    async def RecognizeStreaming(self, requests, context):
        index = len(self.requests)
        received = []
        self.requests.append(received)
        self.metadata.append(dict(context.invocation_metadata()))
        self.active += 1
        try:
            async for request in requests:
                received.append(request)
                if self.fail:
                    await context.abort(grpc.StatusCode.UNAUTHENTICATED, "secret provider details")
                if request.WhichOneof("Event") == "chunk":
                    self.audio.set()
                    yield p.StreamingResponse(
                        partial=p.AlternativeUpdate(
                            alternatives=[p.Alternative(text="unstable hypothesis")]
                        )
                    )
                if request.WhichOneof("Event") == "eou":
                    if self.hang:
                        await asyncio.Future()
                    if index == 0:
                        await asyncio.sleep(self.delay_first)
                    yield p.StreamingResponse(
                        final=p.AlternativeUpdate(
                            alternatives=[p.Alternative(text=f"fixture {index}")]
                        )
                    )
                    if self.informational:
                        # Server housekeeping carries no transcript and must not
                        # fail the utterance: session id first, then a message
                        # with no event set at all.
                        yield p.StreamingResponse(
                            session_uuid=p.SessionUuid(uuid="fixture-session")
                        )
                        yield p.StreamingResponse()
                    if self.refinement is not None:
                        yield p.StreamingResponse(
                            final_refinement=p.FinalRefinement(
                                final_index=self.refinement_index,
                                normalized_text=p.AlternativeUpdate(
                                    alternatives=[p.Alternative(text=self.refinement)]
                                ),
                            )
                        )
        finally:
            self.active -= 1
            self.closed.set()


@pytest.fixture
async def provider():
    server = grpc.aio.server()
    fake = Recognizer()
    service.add_RecognizerServicer_to_server(fake, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channels = []

    def connect():
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        channels.append(channel)
        return channel, service.RecognizerStub(channel)

    yield fake, connect
    for channel in channels:
        await channel.close()
    await server.stop(None)


async def wait_until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.005)


async def utterance(stt):
    await stt.feed_audio(pcm(1800, 200))
    await stt.feed_audio(pcm(0, 500))


async def test_burst_packet_cannot_move_final_speech_clock_backwards(provider, monkeypatch):
    monkeypatch.setattr(speechkit, "time", SimpleNamespace(monotonic=lambda: 100.0))
    fake, connect = provider
    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    finals = []

    async def final(text):
        finals.append((text, stt.final_speech_at))

    await stt.start_stream(on_final=final)
    try:
        await stt.feed_audio(pcm(1000, 200))
        observed = stt.last_speech_at
        await stt.feed_audio(pcm(1000, 200) + pcm(0, 500))
        assert stt.last_speech_at == observed
        await wait_until(lambda: finals)
        assert finals == [("fixture 0", observed)]
        data = b"".join(r.chunk.data for r in fake.requests[0] if r.WhichOneof("Event") == "chunk")
        assert data == pcm(1000, 400) + pcm(0, 500)
    finally:
        await stt.abort_stream()
    assert not stt._tasks and not stt._rpcs


async def test_audio_streams_during_speech_but_only_confirmed_final_is_admitted(provider):
    fake, connect = provider
    finals, partials = [], []

    async def final(text):
        finals.append(text)

    async def partial(text):
        partials.append(text)

    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream(on_partial=partial, on_final=final)
    try:
        await stt.feed_audio(pcm(0, 200))
        await stt.feed_audio(pcm(1800, 200))
        await asyncio.wait_for(fake.audio.wait(), 1)
        assert not finals and not partials
        assert stt.last_speech_at is not None
        await stt.feed_audio(pcm(0, 300))
        await stt.feed_audio(pcm(1800, 200))  # Continue inside local 500 ms pause.
        assert not finals
        await stt.feed_audio(pcm(0, 500))
        await wait_until(lambda: finals)
        assert finals == ["fixture 0"]
        assert stt.final_speech_at == stt.last_speech_at
        assert len(fake.requests) == 1
        requests = fake.requests[0]
        options = requests[0].session_options
        model = options.recognition_model
        assert model.audio_format.raw_audio.sample_rate_hertz == 8000
        assert model.audio_format.raw_audio.audio_channel_count == 1
        assert model.audio_processing_type == p.RecognitionModelOptions.REAL_TIME
        assert (
            model.text_normalization.text_normalization
            == p.TextNormalizationOptions.TEXT_NORMALIZATION_DISABLED
        )
        assert options.eou_classifier.WhichOneof("Classifier") == "external_classifier"
        assert requests[-1].WhichOneof("Event") == "eou"
        data = b"".join(r.chunk.data for r in requests if r.WhichOneof("Event") == "chunk")
        assert data == pcm(0, 200) + pcm(1800, 200) + pcm(0, 300) + pcm(1800, 200) + pcm(0, 500)
        assert fake.metadata[0]["x-data-logging-enabled"] == "false"
        assert fake.metadata[0]["authorization"] == "Api-Key fixture-secret"
    finally:
        await stt.abort_stream()
    assert not stt._tasks and not stt._rpcs


async def test_noise_is_not_sent_and_hangup_never_flushes_a_partial_utterance(provider):
    fake, connect = provider
    stt = SpeechKitSTT(
        SpeechKitSettings("fixture-secret"),
        vad=VADSettings(min_speech_ms=120),
        connection_factory=connect,
    )
    await stt.start_stream()
    await stt.feed_audio(pcm(1800, 40))
    await stt.feed_audio(pcm(0, 500))
    assert not fake.requests
    await stt.feed_audio(pcm(1800, 120))
    await asyncio.wait_for(fake.audio.wait(), 1)
    await stt.abort_stream()
    await wait_until(lambda: fake.active == 0)
    assert all(r.WhichOneof("Event") != "eou" for r in fake.requests[0])
    assert not stt._tasks and not stt._rpcs
    await stt.abort_stream()


async def test_concurrent_rpc_finals_preserve_acoustic_order(provider):
    fake, connect = provider
    fake.delay_first = 0.1
    finals = []

    async def final(text):
        finals.append(text)

    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream(on_final=final)
    try:
        await utterance(stt)
        await utterance(stt)
        await wait_until(lambda: len(finals) == 2)
        assert finals == ["fixture 0", "fixture 1"]
    finally:
        await stt.abort_stream()


@pytest.mark.parametrize("failure", ["denied", "deadline"])
async def test_provider_failure_has_no_retry_or_late_final_or_secret_logging(
    provider, caplog, failure
):
    fake, connect = provider
    fake.fail = failure == "denied"
    fake.hang = failure == "deadline"
    finals = []

    async def final(text):
        finals.append(text)

    stt = SpeechKitSTT(
        SpeechKitSettings("fixture-secret", final_timeout=0.08), connection_factory=connect
    )
    await stt.start_stream(on_final=final)
    try:
        try:
            await utterance(stt)
        except RuntimeError:
            pass
        await wait_until(lambda: stt._error)
        with pytest.raises(RuntimeError, match="unavailable"):
            await stt.feed_audio(pcm(0, 20))
        assert not finals and len(fake.requests) == 1
        assert "fixture-secret" not in caplog.text and "secret provider details" not in caplog.text
    finally:
        await stt.abort_stream()
    assert not stt._tasks and not stt._rpcs


async def test_final_refinement_replaces_raw_segment_and_ignores_housekeeping(
    provider,
):
    fake, connect = provider
    fake.informational = True
    fake.refinement = "Fixture 0!"
    finals = []

    async def final(text):
        finals.append(text)

    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream(on_final=final)
    try:
        await utterance(stt)
        await wait_until(lambda: finals)
        assert finals == ["Fixture 0!"]
    finally:
        await stt.abort_stream()
    assert not stt._tasks and not stt._rpcs


async def test_refinement_out_of_range_fails_explicitly_without_final(provider):
    fake, connect = provider
    fake.refinement = "unbounded"
    fake.refinement_index = 99
    finals = []

    async def final(text):
        finals.append(text)

    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream(on_final=final)
    try:
        try:
            await utterance(stt)
        except RuntimeError:
            pass
        await wait_until(lambda: stt._error)
        assert not finals
    finally:
        await stt.abort_stream()
    assert not stt._tasks and not stt._rpcs


async def test_continuous_speech_beyond_two_seconds_keeps_streaming(provider):
    fake, connect = provider
    finals = []

    async def final(text):
        finals.append(text)

    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream(on_final=final)
    try:
        # A single breathless phrase longer than the old 100-frame bound.
        await stt.feed_audio(pcm(1800, 3000))
        await stt.feed_audio(pcm(0, 600))
        await wait_until(lambda: finals)
        assert finals == ["fixture 0"]
    finally:
        await stt.abort_stream()
    assert not stt._tasks and not stt._rpcs


async def test_audio_backlog_overflow_reports_frames_and_limit():
    events = []

    def observer(event, **values):
        events.append((event, values))

    stt = SpeechKitSTT(
        SpeechKitSettings("fixture-secret", max_audio_queue_frames=32),
        emit=observer,
    )
    utterance = _Utterance(audio=asyncio.Queue(32))
    for _ in range(32):
        utterance.audio.put_nowait(b"\x00" * 320)
    with pytest.raises(RuntimeError, match="backlog"):
        stt._enqueue(utterance, b"\x00" * 320)
    assert events == [("stt_audio_backlog", {"frames": 32, "limit": 32})]


def test_audio_queue_bound_covers_full_utterance_but_stays_finite():
    assert SpeechKitSettings("fixture-secret").max_audio_queue_frames >= 1500
    with pytest.raises(ValueError):
        SpeechKitSettings("fixture-secret", max_audio_queue_frames=4097)


def test_bad_speechkit_configuration_is_rejected():
    for changes in [
        {"api_key": ""},
        {"language": "be-BY"},
        {"model": "unknown"},
        {"max_pending": 99},
    ]:
        with pytest.raises(ValueError):
            SpeechKitSettings(**{"api_key": "fixture-secret", **changes})
    assert "fixture-secret" not in repr(SpeechKitSettings("fixture-secret"))
