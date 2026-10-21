"""Explicit private speech endpoint. No model weights, retries or fallback providers."""

import asyncio
import ipaddress
import json
from contextlib import aclosing
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from ..audio import pcm_to_wav, validate_pcm
from ..batch_stt import BatchSTT
from ..completion import deadline_timeout
from ..interfaces import BaseTTS, TTSCapabilities
from ..speech_text import prepare_tts_text
from .decoder import decode_audio_stream
from .pcm_resampler import resample_pcm_stream


@dataclass(frozen=True)
class PCMHTTPSettings:
    base_url: str
    model: str
    api_key: str = field(default="", repr=False)
    language: str = "ru"
    voice: str = ""
    sample_rate: int = 8000
    timeout: float = 120
    max_response_bytes: int = 65536
    max_text_chars: int = 2000
    max_input_bytes: int = 3_000_000
    max_pcm_bytes: int = 1_920_000
    pronounce_booking_values: bool = False
    expand_byn: bool = False

    def __post_init__(self):
        try:
            url = urlsplit(self.base_url)
            port = url.port
            host = url.hostname
            local = host == "localhost" or bool(host and ipaddress.ip_address(host).is_private)
            local = local and (port is None or 1 <= port <= 65535)
        except ValueError:
            local = False
        if (
            not local
            or url.scheme not in {"http", "https"}
            or url.username
            or url.password
            or url.query
            or url.fragment
            or url.path not in {"", "/", "/v1", "/v1/"}
            or not self.base_url.isascii()
            or any(ord(c) <= 32 for c in self.base_url)
            or not self.model
            or len(self.model) > 200
            or self.sample_rate not in {8000, 16000, 24000, 48000}
            or not 0 < self.timeout <= 600
            or not 1 <= self.max_response_bytes <= 65536
            or not 1 <= self.max_text_chars <= 4000
            or not 2 <= self.max_input_bytes <= 8 * 1024 * 1024
            or not 2 <= self.max_pcm_bytes <= 8 * 1024 * 1024
            or self.language not in {"", "ru", "en", "be"}
            or not self.api_key.isascii()
            or any(ord(c) < 32 for c in self.api_key)
        ):
            raise ValueError("Invalid private speech endpoint configuration")


class _Client:
    def __init__(self, settings, factory):
        self.settings, self.factory = settings, factory
        self.client, self.closed = None, False

    def get(self):
        if self.closed:
            raise RuntimeError("Speech endpoint is closed")
        if self.client is None:
            import httpx

            self.client = (self.factory or httpx.AsyncClient)(
                timeout=self.settings.timeout,
                trust_env=False,
                follow_redirects=False,
                limits=httpx.Limits(max_connections=2, max_keepalive_connections=2),
            )
        return self.client

    @property
    def headers(self):
        return {"Authorization": "Bearer " + self.settings.api_key} if self.settings.api_key else {}

    async def close(self):
        self.closed = True
        client, self.client = self.client, None
        if client is not None:
            await client.aclose()


def _check(response, mime):
    if response.status_code != 200:
        raise RuntimeError("Speech endpoint request failed")
    if response.headers.get("content-type", "").split(";", 1)[0].lower() not in mime:
        raise ValueError("Unexpected speech endpoint response type")


class PCMHTTPSTT(BatchSTT):
    def __init__(self, settings, *, client_factory=None, **kwargs):
        super().__init__(**kwargs)
        self.settings = settings
        self.http = _Client(settings, client_factory)

    async def _open(self):
        self.http.get()

    async def _transcribe(self, pcm):
        validate_pcm(pcm)
        if not pcm:
            return ""
        if len(pcm) > self.queue.max_utterance_bytes:
            raise ValueError("STT audio exceeds configured limit")
        body = {"model": self.settings.model, "response_format": "json"}
        if self.settings.language:
            body["language"] = self.settings.language
        async with asyncio.timeout(self.settings.timeout):
            async with self.http.get().stream(
                "POST",
                self.settings.base_url.rstrip("/") + "/audio/transcriptions",
                data=body,
                files={"file": ("utterance.wav", pcm_to_wav(pcm), "audio/wav")},
                headers=self.http.headers,
            ) as response:
                _check(response, {"application/json"})
                raw = bytearray()
                async for part in response.aiter_bytes(65536):
                    if len(raw) + len(part) > self.settings.max_response_bytes:
                        raise ValueError("STT response exceeds configured limit")
                    raw.extend(part)
        data = json.loads(raw)
        if not isinstance(data, dict) or not isinstance(data.get("text"), str):
            raise ValueError("Invalid speech endpoint transcript")
        if len(data["text"]) > self.queue.max_final_chars:
            raise ValueError("STT transcript exceeds configured limit")
        return data["text"].strip()

    async def _close(self):
        await self.http.close()


class PCMHTTPTTS(BaseTTS):
    capabilities = TTSCapabilities(streaming_synthesis=True)

    def __init__(self, settings, *, client_factory=None):
        self.settings = settings
        self.http = _Client(settings, client_factory)

    async def synthesize(self, text):
        async with aclosing(self.synthesize_stream(text)) as stream:
            return b"".join([part async for part in stream])

    async def synthesize_stream(self, text):
        text = prepare_tts_text(
            text,
            booking_values=self.settings.pronounce_booking_values,
            expand_byn=self.settings.expand_byn,
        ).strip()
        if not text:
            return
        if len(text) > self.settings.max_text_chars:
            raise ValueError("TTS text exceeds configured limit")
        deadline = asyncio.get_running_loop().time() + self.settings.timeout
        body = {
            "model": self.settings.model,
            "input": text,
            "voice": self.settings.voice,
            "response_format": "pcm",
            "sample_rate": self.settings.sample_rate,
        }
        async with deadline_timeout(deadline):
            context = self.http.get().stream(
                "POST",
                self.settings.base_url.rstrip("/") + "/audio/speech",
                json=body,
                headers=self.http.headers,
            )
            response = await context.__aenter__()
        try:
            _check(response, {"audio/pcm"})
            fields = response.headers.get_list("content-type")
            parameters = [p.strip().lower().split("=", 1) for p in fields[0].split(";")[1:]]
            if (
                len(fields) != 1
                or len(parameters) != 2
                or any(len(p) != 2 for p in parameters)
                or dict(parameters) != {"rate": str(self.settings.sample_rate), "channels": "1"}
            ):
                raise ValueError("Unsupported private speech PCM format")
            if response.headers.get("content-encoding", "identity").lower() != "identity":
                raise ValueError("Compressed speech PCM is unsupported")
            if response.headers.get("x-pcm-sample-rate", str(self.settings.sample_rate)) != str(
                self.settings.sample_rate
            ):
                raise ValueError("Unexpected speech PCM sample rate")

            async def source():
                received, carry = 0, b""
                async with aclosing(response.aiter_raw()) as chunks:
                    while True:
                        async with deadline_timeout(deadline):
                            try:
                                part = await anext(chunks)
                            except StopAsyncIteration:
                                break
                        if len(part) > 65536:
                            raise ValueError("Speech PCM chunk exceeds configured limit")
                        received += len(part)
                        if received > self.settings.max_input_bytes:
                            raise ValueError("TTS input exceeds configured limit")
                        data = carry + part
                        end = len(data) // 2 * 2
                        carry = data[end:]
                        if end:
                            yield data[:end]
                if carry or not received:
                    raise ValueError("Empty or incomplete speech PCM")

            async with aclosing(source()) as raw_pcm:
                if self.settings.sample_rate == 8000:
                    converted = raw_pcm
                else:
                    converter = (
                        resample_pcm_stream
                        if self.settings.sample_rate == 24000
                        else decode_audio_stream
                    )
                    converted = converter(
                        raw_pcm,
                        input_pcm_rate=self.settings.sample_rate,
                        max_input_bytes=self.settings.max_input_bytes,
                        max_output_bytes=self.settings.max_pcm_bytes,
                        timeout=max(0.000001, deadline - asyncio.get_running_loop().time()),
                    )
                produced = 0
                async with aclosing(converted) as stream:
                    async for part in stream:
                        produced += len(part)
                        if produced > self.settings.max_pcm_bytes:
                            raise ValueError("TTS PCM exceeds configured limit")
                        yield part
        finally:
            await context.__aexit__(None, None, None)

    async def close(self):
        await self.http.close()
