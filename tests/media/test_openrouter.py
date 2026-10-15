"""OpenRouter wire contracts with injected HTTP/decoder fakes, never paid APIs."""

import asyncio
import base64
import io
import json
import wave
from contextlib import aclosing

import httpx
import pytest

from tests.media.media_fakes import pcm
from voice_gateway.media.batch_stt import BatchSTTSettings
from voice_gateway.media.providers.openrouter import (
    BASE_URL,
    OpenRouterError,
    OpenRouterSTT,
    OpenRouterSTTSettings,
    OpenRouterTTS,
    OpenRouterTTSSettings,
)


def factory(handler, observed):
    def create(**kwargs):
        observed.append(kwargs)
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler), **kwargs)
        observed.append(client)
        return client

    return create


async def test_stt_sends_bounded_wav_json_and_reuses_connection_without_retries():
    requests, clients = [], []

    def handler(request):
        requests.append(request)
        body = json.loads(request.content)
        assert str(request.url) == BASE_URL + "/audio/transcriptions"
        assert body["model"] == "fixture/asr" and body["language"] == "ru"
        assert request.headers["authorization"] == "Bearer fixture-key"
        with wave.open(io.BytesIO(base64.b64decode(body["input_audio"]["data"]))) as audio:
            assert audio.getparams()[:3] == (1, 2, 8000)
            assert audio.readframes(10000) == pcm(1000)
        return httpx.Response(200, json={"text": "  fixture  "})

    stt = OpenRouterSTT(
        OpenRouterSTTSettings("fixture-key", "fixture/asr", language="ru"),
        client_factory=factory(handler, clients),
    )
    await stt.start_stream()
    assert await stt._transcribe(pcm()) == "fixture"
    assert await stt._transcribe(pcm()) == "fixture"
    assert len(clients) == 2 and len(requests) == 2
    assert not clients[0]["follow_redirects"] and not clients[0]["trust_env"]
    await stt.stop_stream()
    assert clients[1].is_closed
    assert "fixture-key" not in repr(stt.settings)


@pytest.mark.parametrize("status", [302, 401, 402, 429, 500])
async def test_stt_does_not_retry_redirect_or_expose_provider_error_body(status):
    requests, clients = [], []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            status,
            headers={"location": "https://example.invalid"},
            json={"error": "secret fixture transcript"},
        )

    stt = OpenRouterSTT(
        OpenRouterSTTSettings("fixture-key", "fixture/asr"),
        client_factory=factory(handler, clients),
    )
    try:
        with pytest.raises(OpenRouterError) as error:
            await stt._transcribe(pcm())
        assert error.value.status == status and len(requests) == 1
        assert "secret" not in str(error.value)
    finally:
        await stt.stop_stream()


@pytest.mark.parametrize("body", [{"text": 3}, {}, ["text"], {"text": "x" * 200}])
async def test_stt_rejects_invalid_or_oversized_transcript(body):
    stt = OpenRouterSTT(
        OpenRouterSTTSettings("fixture-key", "fixture/asr"),
        client_factory=factory(lambda req: httpx.Response(200, json=body), []),
        queue=BatchSTTSettings(max_final_chars=100),
    )
    try:
        with pytest.raises(ValueError):
            await stt._transcribe(pcm())
    finally:
        await stt.stop_stream()


async def test_stt_response_limit_closes_http_stream():
    responses = []

    def handler(request):
        response = httpx.Response(200, json={"text": "x" * 1000})
        responses.append(response)
        return response

    stt = OpenRouterSTT(
        OpenRouterSTTSettings("fixture-key", "fixture/asr", max_response_bytes=32),
        client_factory=factory(handler, []),
    )
    try:
        with pytest.raises(ValueError, match="response exceeds"):
            await stt._transcribe(pcm())
        assert responses[0].is_closed
    finally:
        await stt.stop_stream()


class AudioStream(httpx.AsyncByteStream):
    def __init__(self, *, stall=False):
        self.closed = False
        self.stall = stall

    async def __aiter__(self):
        yield b"m" * 1024
        if self.stall:
            await asyncio.Future()
        yield b"p" * 1024

    async def aclose(self):
        self.closed = True


async def decode_fixture(chunks, **kwargs):
    async for chunk in chunks:
        yield pcm(1000)


async def test_tts_yields_before_response_end_and_cancellation_closes_http_and_decoder():
    stream, clients, requests = AudioStream(stall=True), [], []
    decoder_closed = asyncio.Event()

    def handler(request):
        requests.append(request)
        return httpx.Response(200, headers={"content-type": "audio/mpeg"}, stream=stream)

    async def decode(chunks, **kwargs):
        try:
            async for chunk in chunks:
                yield pcm(1000)
        finally:
            decoder_closed.set()

    tts = OpenRouterTTS(
        OpenRouterTTSSettings("fixture-key", "fixture/tts", voice="fixture-voice"),
        client_factory=factory(handler, clients),
        decoder=decode,
    )
    output = tts.synthesize_stream("Fixture")
    assert await asyncio.wait_for(anext(output), 1) == pcm(1000)
    body = json.loads(requests[0].content)
    assert body == {
        "model": "fixture/tts",
        "input": "Fixture",
        "response_format": "mp3",
        "voice": "fixture-voice",
    }
    assert str(requests[0].url) == BASE_URL + "/audio/speech"
    blocked = asyncio.create_task(anext(output))
    await asyncio.sleep(0)
    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked
    assert stream.closed and decoder_closed.is_set()
    await tts.close()
    assert clients[1].is_closed


@pytest.mark.parametrize("mime", ["application/json", "audio/wav", "text/plain"])
async def test_tts_rejects_wrong_audio_type(mime):
    tts = OpenRouterTTS(
        OpenRouterTTSSettings("fixture-key", "fixture/tts"),
        client_factory=factory(
            lambda req: httpx.Response(200, headers={"content-type": mime}, content=b"error"), []
        ),
        decoder=decode_fixture,
    )
    try:
        with pytest.raises(ValueError, match="response type"):
            await tts.synthesize("Fixture")
    finally:
        await tts.close()


@pytest.mark.parametrize("limit", ["mp3", "pcm"])
async def test_tts_enforces_audio_bounds_and_closes_response(limit):
    stream = AudioStream()
    settings = OpenRouterTTSSettings(
        "fixture-key",
        "fixture/tts",
        max_mp3_bytes=100 if limit == "mp3" else 3000,
        max_pcm_bytes=100 if limit == "pcm" else 3200,
    )
    tts = OpenRouterTTS(
        settings,
        client_factory=factory(
            lambda req: httpx.Response(200, headers={"content-type": "audio/mpeg"}, stream=stream),
            [],
        ),
        decoder=decode_fixture,
    )
    try:
        with pytest.raises(ValueError, match="exceeds"):
            await tts.synthesize("Fixture")
        assert stream.closed
    finally:
        await tts.close()


async def test_tts_timeout_closes_response_without_fallback():
    stream = AudioStream(stall=True)
    tts = OpenRouterTTS(
        OpenRouterTTSSettings("fixture-key", "fixture/tts", stream_timeout=0.02),
        client_factory=factory(
            lambda req: httpx.Response(200, headers={"content-type": "audio/mpeg"}, stream=stream),
            [],
        ),
        decoder=decode_fixture,
    )
    try:
        with pytest.raises(TimeoutError):
            await tts.synthesize("Fixture")
        assert stream.closed
    finally:
        await tts.close()


@pytest.mark.parametrize("separate_tasks", [False, True])
@pytest.mark.parametrize("stall", [False, True])
async def test_stream_deadline_is_owned_by_reads_not_the_consumer(separate_tasks, stall):
    stream = AudioStream(stall=stall)
    tts = OpenRouterTTS(
        OpenRouterTTSSettings("fixture-key", "fixture/tts", stream_timeout=0.04),
        client_factory=factory(
            lambda req: httpx.Response(200, headers={"content-type": "audio/mpeg"}, stream=stream),
            [],
        ),
        decoder=decode_fixture,
    )

    async def consume():
        async with aclosing(tts.synthesize_stream("Fixture")) as output:
            first = asyncio.create_task(anext(output)) if separate_tasks else anext(output)
            assert await first == pcm(1000)
            # A provider deadline must not cancel the caller while it handles
            # the already-yielded frame, and must survive a change of read task.
            await asyncio.sleep(0.08)
            read = asyncio.create_task(anext(output))
            try:
                done, _ = await asyncio.wait({read}, timeout=0.1)
                assert done, "The stream deadline disappeared with the first read task"
                with pytest.raises(TimeoutError):
                    await read
            finally:
                read.cancel()
                await asyncio.gather(read, return_exceptions=True)

    try:
        await asyncio.create_task(consume())
        assert stream.closed
    finally:
        await tts.close()
