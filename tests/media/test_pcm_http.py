"""The same bounded speech contract for router and private adapters."""

import asyncio
import json

import httpx
import pytest

from tests.media.media_fakes import pcm
from tests.media.test_routing import config
from voice_gateway.settings import Settings
from voice_gateway.live import build_live_runtime
from voice_gateway.media.providers.openrouter import (
    OpenRouterSTT,
    OpenRouterSTTSettings,
    OpenRouterTTS,
    OpenRouterTTSSettings,
)
from voice_gateway.media.providers.pcm_http import PCMHTTPSettings, PCMHTTPSTT, PCMHTTPTTS
from voice_gateway.media.speech_text import prepare_tts_text


class Audio(httpx.AsyncByteStream):
    def __init__(self, parts, *, wait=False, fail=False):
        self.parts, self.wait, self.fail = parts, wait, fail
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.yielded, self.closed = 0, 0

    async def __aiter__(self):
        self.entered.set()
        if self.wait:
            await self.release.wait()
        for part in self.parts:
            self.yielded += 1
            yield part
        if self.fail:
            raise httpx.ReadError("Synthetic disconnect")

    async def aclose(self):
        self.closed += 1


def tts(kind, audio, *, timeout=2, rate=24000, **options):
    wire = []

    def respond(request):
        wire.append(request)
        return httpx.Response(
            200, headers={"content-type": f"audio/pcm;rate={rate};channels=1"}, stream=audio
        )

    def client(**kwargs):
        return httpx.AsyncClient(**kwargs, transport=httpx.MockTransport(respond))

    if kind == "private":
        adapter = PCMHTTPTTS(
            PCMHTTPSettings(
                "http://127.0.0.1:19382/v1", "test", sample_rate=rate, timeout=timeout, **options
            ),
            client_factory=client,
        )
    else:
        adapter = OpenRouterTTS(
            OpenRouterTTSSettings(
                "synthetic-cloud-key",
                "test",
                response_format="pcm",
                native_pcm_resampler=True,
                stream_timeout=timeout,
                **options,
            ),
            client_factory=client,
        )
    return adapter, wire


@pytest.mark.parametrize("kind", ["private", "router"])
@pytest.mark.parametrize(
    "case",
    ["pcm", "odd", "disconnect", "timeout", "cancel_before", "cancel_during", "slow_consumer"],
)
async def test_tts_contract(kind, case):
    data = b"\x10\0" * 2400
    audio = Audio(
        [data, data], wait=case in {"timeout", "cancel_during"}, fail=case == "disconnect"
    )
    if case == "odd":
        audio.parts = [b"\x10", b"\0\x10"]
    adapter, wire = tts(kind, audio, timeout=0.2 if case == "timeout" else 2)
    try:
        if case == "cancel_before":
            pending = asyncio.create_task(adapter.synthesize("Тест"))
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            assert not wire
        elif case == "cancel_during":
            pending = asyncio.create_task(adapter.synthesize("Тест"))
            await asyncio.wait_for(audio.entered.wait(), 1)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            assert audio.closed == 1
        elif case == "slow_consumer":
            stream = adapter.synthesize_stream("Тест")
            assert await anext(stream)
            before = audio.yielded
            await asyncio.sleep(0)
            assert before == audio.yielded == 1
            await stream.aclose()
            assert audio.closed == 1
        elif case in {"odd", "disconnect", "timeout"}:
            expected = {"odd": ValueError, "disconnect": httpx.ReadError, "timeout": TimeoutError}[
                case
            ]
            with pytest.raises(expected):
                await adapter.synthesize("Тест")
            assert audio.closed == 1
        else:
            result = await adapter.synthesize("Тест")
            assert len(result) == 3200 and any(result)
            assert audio.closed == 1
        assert len(wire) == (0 if case == "cancel_before" else 1)
        if wire and kind == "private":
            assert "authorization" not in wire[0].headers
            body = json.loads(wire[0].content)
            assert not {"provider", "reasoning", "input_audio"} & body.keys()
    finally:
        client = adapter.http.client
        await adapter.close()
        await adapter.close()
        assert client is None or client.is_closed


@pytest.mark.parametrize("kind", ["private", "router"])
async def test_five_independent_speech_streams_close_without_shared_state(kind):
    adapters = [tts(kind, Audio([b"\x10\0" * 2400]))[0] for _ in range(5)]
    try:
        output = await asyncio.gather(*(a.synthesize("Тест") for a in adapters))
        assert len(output) == 5 and all(len(p) == 1600 for p in output)
        assert len({id(a.http.client) for a in adapters}) == 5
    finally:
        clients = [a.http.client for a in adapters]
        await asyncio.gather(*(a.close() for a in adapters))
        assert all(c.is_closed for c in clients)


@pytest.mark.parametrize("rate", [8000, 16000, 24000, 48000])
async def test_private_pcm_rates_reach_the_same_telephone_duration(rate):
    audio = Audio([b"\x10", b"\0" + b"\x10\0" * (rate // 5 - 1)])
    adapter, _ = tts("private", audio, rate=rate)
    try:
        output = await adapter.synthesize("Тест")
        assert 3168 <= len(output) <= 3232 and len(output) % 2 == 0 and any(output)
    finally:
        await adapter.close()
    assert audio.closed == 1


@pytest.mark.parametrize("kind", ["private", "router"])
async def test_stt_contract_bounds_transcript_and_ignores_remote_clock(kind):
    wire = []

    def respond(request):
        wire.append(request)
        return httpx.Response(200, json={"text": "Проверка", "final_speech_at": 9999999999})

    def client(**kwargs):
        return httpx.AsyncClient(**kwargs, transport=httpx.MockTransport(respond))

    adapter = (
        PCMHTTPSTT(PCMHTTPSettings("http://127.0.0.1:19382/v1", "test"), client_factory=client)
        if kind == "private"
        else OpenRouterSTT(OpenRouterSTTSettings("cloud-key", "test"), client_factory=client)
    )
    try:
        assert await adapter._transcribe(pcm(1000, 200)) == "Проверка"
        assert adapter.final_speech_at is None  # Remote wall clock is never acoustic evidence.
        assert len(wire) == 1
        if kind == "private":
            assert "authorization" not in wire[0].headers
            assert "multipart/form-data" in wire[0].headers["content-type"]
            assert b"RIFF" in wire[0].content and b"cloud-key" not in wire[0].content
    finally:
        await adapter.abort_stream()
        await adapter.abort_stream()


@pytest.mark.parametrize("old_key", ["", "unused-cloud-key"])
async def test_selected_private_adapters_need_no_cloud_key_or_ffmpeg_at_8khz(monkeypatch, old_key):
    monkeypatch.setattr("voice_gateway.live.shutil.which", lambda _: None)
    value = config(
        stt_provider="pcm_http",
        tts_provider="pcm_http",
        stt_api_key=old_key,
        tts_api_key=old_key,
        stt_base_url="http://127.0.0.1:19382/v1",
        tts_base_url="http://127.0.0.1:19382/v1",
        tts_format="pcm",
    )
    runtime = build_live_runtime(Settings(mode="live_audiosocket"), value)
    assert isinstance(runtime.stt_factory(), PCMHTTPSTT)
    assert isinstance(runtime.tts_factory(), PCMHTTPTTS)
    assert runtime.stt_factory().settings.api_key == runtime.tts_factory().settings.api_key == ""
    await runtime.stop()
    with pytest.raises(ValueError, match="Separate boundary credentials"):
        value = config(
            stt_provider="pcm_http",
            stt_api_key="",
            stt_base_url="http://127.0.0.1:19382/v1",
            conversation_token="a" * 32,
        )


@pytest.mark.parametrize("kind", ["private", "router"])
async def test_common_pronunciation_runs_once_on_speech_copy(kind, monkeypatch):
    import voice_gateway.media.speech_text as module

    calls = []
    original = module.pronounce_booking_values

    def count(text):
        calls.append(text)
        return original(text)

    monkeypatch.setattr(module, "pronounce_booking_values", count)
    canonical = "Запись 21.10.2026 в 10:30 (Europe/Minsk), цена 50 BYN."
    expected = prepare_tts_text(canonical, booking_values=True, expand_byn=True)
    calls.clear()
    adapter, wire = tts(
        kind, Audio([b"\x10\0" * 2400]), pronounce_booking_values=True, expand_byn=True
    )
    try:
        await adapter.synthesize(canonical)
        assert calls == [canonical]
        assert json.loads(wire[0].content)["input"] == expected
        assert "Europe/Minsk" in canonical and "Europe/Minsk" not in expected
    finally:
        await adapter.close()


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/v1",
        "http://user:secret@127.0.0.1/v1",
        "http://127.0.0.1/v1?token=secret",
        "http://127.0.0.1:invalid/v1",
        "http://127.0.0.1:0/v1",
    ],
)
def test_private_endpoint_rejects_implicit_public_or_embedded_credentials(url):
    with pytest.raises(ValueError, match="Invalid private speech endpoint"):
        PCMHTTPSettings(url, "test")


@pytest.mark.parametrize(
    "case", ["missing_rate", "wrong_rate", "stereo", "gzip", "input_limit", "output_limit"]
)
async def test_private_pcm_metadata_and_bounds_fail_and_close(case):
    audio = Audio([b"\x10\0" * 2400])
    headers = {"content-type": "audio/pcm;rate=8000;channels=1"}
    if case == "missing_rate":
        headers["content-type"] = "audio/pcm"
    if case == "wrong_rate":
        headers["content-type"] = "audio/pcm;rate=24000;channels=1"
    if case == "stereo":
        headers["content-type"] = "audio/pcm;rate=8000;channels=2"
    if case == "gzip":
        headers["content-encoding"] = "gzip"

    def client(**kwargs):
        return httpx.AsyncClient(
            **kwargs,
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, headers=headers, stream=audio)
            ),
        )

    options = (
        {"max_input_bytes": 100}
        if case == "input_limit"
        else {"max_pcm_bytes": 100}
        if case == "output_limit"
        else {}
    )
    adapter = PCMHTTPTTS(
        PCMHTTPSettings("http://127.0.0.1:19382/v1", "test", **options), client_factory=client
    )
    try:
        with pytest.raises(ValueError):
            await adapter.synthesize("Тест")
        assert audio.closed == 1
    finally:
        await adapter.close()


@pytest.mark.parametrize("case", ["malformed", "wrong_shape", "oversized"])
async def test_private_transcript_rejects_invalid_or_oversized_response(case):
    body = b"{" if case == "malformed" else b'{"text":0}' if case == "wrong_shape" else b"x" * 100
    stream = Audio([body])

    def client(**kwargs):
        return httpx.AsyncClient(
            **kwargs,
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200, headers={"content-type": "application/json"}, stream=stream
                )
            ),
        )

    adapter = PCMHTTPSTT(
        PCMHTTPSettings("http://127.0.0.1:19382/v1", "test", max_response_bytes=50),
        client_factory=client,
    )
    try:
        with pytest.raises(ValueError):
            await adapter._transcribe(pcm(1000, 200))
        assert stream.closed == 1
    finally:
        await adapter.abort_stream()


async def test_private_telephone_pcm_does_not_start_decoder(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Telephone PCM must not start a converter")

    monkeypatch.setattr("voice_gateway.media.providers.pcm_http.decode_audio_stream", forbidden)
    monkeypatch.setattr("voice_gateway.media.providers.pcm_http.resample_pcm_stream", forbidden)
    audio = Audio([pcm(1000, 200)])
    adapter, _ = tts("private", audio, rate=8000)
    try:
        assert await adapter.synthesize("Тест") == pcm(1000, 200)
    finally:
        await adapter.close()


async def test_private_stt_input_limit_rejects_before_http():
    def forbidden(**kwargs):
        raise AssertionError("Oversized utterance must not open HTTP")

    adapter = PCMHTTPSTT(
        PCMHTTPSettings("http://127.0.0.1:19382/v1", "test", max_input_bytes=100),
        client_factory=forbidden,
    )
    try:
        with pytest.raises(ValueError):
            await adapter._transcribe(pcm(1000, 200))
    finally:
        await adapter.abort_stream()
