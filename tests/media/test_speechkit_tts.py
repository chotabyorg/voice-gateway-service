"""Real loopback gRPC exercises SpeechKit streaming PCM and cancellation."""

import asyncio
from contextlib import aclosing

import grpc
import pytest
from yandex.cloud.ai.tts.v3 import tts_pb2 as p
from yandex.cloud.ai.tts.v3 import tts_service_pb2_grpc as service

from voice_gateway.media.providers.speechkit import SpeechKitTTS, SpeechKitTTSSettings


class Synthesizer(service.SynthesizerServicer):
    def __init__(self):
        self.requests = []
        self.metadata = []
        self.chunks = [b"\x01", b"\x00\x02\x00"]
        self.block = False
        self.fail_after_audio = False
        self.closed = asyncio.Event()

    async def StreamSynthesis(self, requests, context):
        received = []
        self.requests.append(received)
        self.metadata.append(dict(context.invocation_metadata()))
        try:
            async for request in requests:
                received.append(request)
            # LiveTTS emits text separately. It must not be mistaken for empty audio.
            yield p.StreamSynthesisResponse(chunk_type=p.StreamSynthesisResponse.TEXT_ONLY)
            for data in self.chunks:
                yield p.StreamSynthesisResponse(
                    audio_chunk=p.AudioChunk(data=data),
                    chunk_type=p.StreamSynthesisResponse.AUDIO_ONLY,
                )
            if self.fail_after_audio:
                await context.abort(grpc.StatusCode.INTERNAL, "secret input / credentials")
            if self.block:
                await asyncio.Future()
        finally:
            self.closed.set()


@pytest.fixture
async def provider():
    server = grpc.aio.server()
    fake = Synthesizer()
    service.add_SynthesizerServicer_to_server(fake, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channels = []

    def connect():
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        channels.append(channel)
        return channel, service.SynthesizerStub(channel)

    yield fake, connect, channels
    for channel in channels:
        await channel.close()
    await server.stop(None)


async def test_livetts_stream_pcm_currency_and_reuses_tls_channel(provider):
    fake, connect, channels = provider
    tts = SpeechKitTTS(
        SpeechKitTTSSettings("fixture-secret", expand_byn=True), connection_factory=connect
    )
    try:
        for _ in range(2):
            assert await tts.synthesize("Стоимость 30 BYN.") == b"\x01\x00\x02\x00"
        assert len(channels) == 1
        options = fake.requests[0][0].options
        assert (options.model, options.voice, options.role) == ("livetts", "sofia", "support")
        assert options.output_audio_spec.raw_audio.sample_rate_hertz == 8000
        assert options.loudness_normalization_type == p.LUFS
        assert not options.speed and not options.pitch_shift
        assert "белорусских рублей" in fake.requests[0][1].synthesis_input.text
        assert fake.requests[0][2].HasField("force_synthesis")
        assert fake.metadata[0]["x-data-logging-enabled"] == "false"
    finally:
        await tts.close()
    with pytest.raises(RuntimeError, match="unavailable"):
        await tts.synthesize("Closed")


async def test_prewarm_connects_once_before_first_answer(provider):
    fake, connect, channels = provider
    tts = SpeechKitTTS(SpeechKitTTSSettings("fixture"), connection_factory=connect)
    try:
        tts.prewarm()
        tts.prewarm()
        await asyncio.wait_for(tts._warming, 1)
        assert channels[0].get_state() == grpc.ChannelConnectivity.READY
        assert await tts.synthesize("Test") == b"\x01\x00\x02\x00"
        assert len(channels) == 1 and len(fake.requests) == 1
    finally:
        await tts.close()
    tts.prewarm()
    assert len(channels) == 1


async def test_booking_values_reach_provider_as_speech_without_changing_input(provider):
    fake, connect, _ = provider
    tts = SpeechKitTTS(SpeechKitTTSSettings("fixture", expand_byn=True), connection_factory=connect)
    canonical = "19.10.2026 в 18:00 (Europe/Minsk), 22.00 BYN, контакт +375291112233."
    try:
        assert await tts.synthesize(canonical) == b"\x01\x00\x02\x00"
        assert fake.requests[0][1].synthesis_input.text == (
            "19 октября 2026 года в 18 часов по минскому времени, "
            "22 белорусских рубля, контакт плюс три семь пять два девять один один один два два три три."
        )
        assert canonical == "19.10.2026 в 18:00 (Europe/Minsk), 22.00 BYN, контакт +375291112233."
    finally:
        await tts.close()


@pytest.mark.parametrize("limit,count", [(30, 1), (4000, 50)])
async def test_expanded_text_limits_are_checked_before_provider_connection(provider, limit, count):
    _, connect, channels = provider
    tts = SpeechKitTTS(
        SpeechKitTTSSettings("fixture", max_text_chars=limit), connection_factory=connect
    )
    # The second case passes the character bound but exceeds the UTF-8 byte bound.
    text = "+375291112233 " * count
    try:
        with pytest.raises(ValueError, match="text exceeds limit"):
            await tts.synthesize(text)
        assert not channels
    finally:
        await tts.close()


async def test_first_pcm_precedes_completion_and_barge_in_cancels_rpc(provider):
    fake, connect, _ = provider
    fake.block = True
    tts = SpeechKitTTS(SpeechKitTTSSettings("fixture"), connection_factory=connect)
    async with aclosing(tts.synthesize_stream("Test")) as stream:
        assert await asyncio.wait_for(anext(stream), 1) == b"\x01\x00\x02\x00"
        assert not fake.closed.is_set()
    await asyncio.wait_for(fake.closed.wait(), 1)
    assert not tts._rpcs
    await tts.close()


async def test_mid_stream_provider_failure_does_not_claim_completion_or_expose_details(provider):
    fake, connect, _ = provider
    fake.fail_after_audio = True
    tts = SpeechKitTTS(SpeechKitTTSSettings("fixture"), connection_factory=connect)
    try:
        async with aclosing(tts.synthesize_stream("Test")) as stream:
            assert await anext(stream)
            with pytest.raises(RuntimeError, match="^SpeechKit TTS stream failed$"):
                await anext(stream)
        assert not tts._rpcs
    finally:
        await tts.close()


@pytest.mark.parametrize("chunks,limit", [([], 100), ([b"a"], 100), ([b"a" * 102], 100)])
async def test_empty_truncated_and_excess_audio_fail(provider, chunks, limit):
    fake, connect, _ = provider
    fake.chunks = chunks
    tts = SpeechKitTTS(
        SpeechKitTTSSettings("fixture", max_pcm_bytes=limit), connection_factory=connect
    )
    try:
        with pytest.raises(RuntimeError, match="stream failed"):
            await tts.synthesize("Test")
        assert not tts._rpcs
    finally:
        await tts.close()


async def test_stalled_stream_has_deadline_and_close_is_safe(provider):
    fake, connect, _ = provider
    fake.chunks, fake.block = [], True
    tts = SpeechKitTTS(
        SpeechKitTTSSettings("fixture", stream_timeout=0.05), connection_factory=connect
    )
    try:
        with pytest.raises(RuntimeError, match="stream failed"):
            await asyncio.wait_for(tts.synthesize("Test"), 1)
        await asyncio.wait_for(fake.closed.wait(), 1)
    finally:
        await tts.close()
        await tts.close()


async def test_hangup_cancels_pending_receive(provider):
    fake, connect, _ = provider
    fake.chunks, fake.block = [], True
    tts = SpeechKitTTS(SpeechKitTTSSettings("fixture"), connection_factory=connect)
    task = asyncio.create_task(tts.synthesize("Test"))
    async with asyncio.timeout(1):
        while not fake.requests:
            await asyncio.sleep(0.005)
    await tts.close()
    with pytest.raises(RuntimeError, match="provider cancelled"):
        await asyncio.wait_for(task, 1)
    assert not tts._rpcs


@pytest.mark.parametrize(
    "changes", [{"model": "general"}, {"voice": "marina"}, {"role": "whisper"}, {"api_key": ""}]
)
def test_model_voice_families_and_roles_must_match(changes):
    with pytest.raises(ValueError):
        SpeechKitTTSSettings(**{"api_key": "fixture", **changes})
