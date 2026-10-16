"""Explicit OpenRouter audio adapters, adapted from MTS provider mechanics.

Only speech/transcription endpoints are used. No model fallback, chat, prompts,
history, or application activation. HTTP clients and decoder are injectable.
"""

import asyncio
import base64
import json
from contextlib import AsyncExitStack, aclosing
from dataclasses import dataclass, field

from ..audio import pcm_to_wav, validate_pcm
from ..batch_stt import BatchSTT
from ..completion import deadline_timeout
from ..interfaces import BaseTTS
from ..speech_text import pronounce_byn
from .decoder import decode_audio_stream
from .pcm_resampler import resample_pcm_stream

BASE_URL = "https://openrouter.ai/api/v1"


@dataclass(frozen=True)
class OpenRouterSettings:
    api_key: str = field(repr=False)
    model: str
    timeout: float = 25

    def __post_init__(self):
        if not self.api_key or not self.api_key.isascii() or not self.model or self.timeout <= 0:
            raise ValueError("Invalid OpenRouter settings")


@dataclass(frozen=True)
class OpenRouterSTTSettings(OpenRouterSettings):
    language: str = ""
    max_response_bytes: int = 131072

    def __post_init__(self):
        super().__post_init__()
        if not 1 <= self.max_response_bytes <= 1_000_000:
            raise ValueError("Invalid STT response limit")


@dataclass(frozen=True)
class OpenRouterTTSSettings(OpenRouterSettings):
    voice: str = ""
    response_format: str = "mp3"
    expand_byn: bool = False
    max_text_chars: int = 2000
    max_mp3_bytes: int = 3_000_000
    max_pcm_bytes: int = 1_920_000
    stream_timeout: float = 120
    native_pcm_resampler: bool = False

    def __post_init__(self):
        super().__post_init__()
        if (
            not 1 <= self.max_text_chars <= 4000
            or not 1 <= self.max_mp3_bytes <= 8 * 1024 * 1024
            or not 2 <= self.max_pcm_bytes <= 8 * 1024 * 1024
            or not 0 < self.stream_timeout <= 600
            or self.response_format not in {"mp3", "pcm"}
        ):
            raise ValueError("Invalid TTS limits")


class OpenRouterError(RuntimeError):
    """Expose only HTTP status, never provider bodies, transcripts, or credentials."""

    def __init__(self, status):
        super().__init__(f"OpenRouter audio request failed (HTTP {status})")
        self.status = status


class _AudioClient:
    def __init__(self, settings, client_factory=None):
        self.settings, self.factory = settings, client_factory
        self.client = None
        self.closed = False

    def get(self):
        if self.closed:
            raise RuntimeError("OpenRouter audio client is closed")
        if self.client is None:
            import httpx

            factory = self.factory or httpx.AsyncClient
            self.client = factory(
                timeout=self.settings.timeout,
                follow_redirects=False,
                trust_env=False,
                limits=httpx.Limits(
                    max_connections=2, max_keepalive_connections=2, keepalive_expiry=120
                ),
            )
        return self.client

    @property
    def headers(self):
        return {"Authorization": "Bearer " + self.settings.api_key}

    async def close(self):
        self.closed = True
        client, self.client = self.client, None
        if client is not None:
            await client.aclose()


def _check_response(response, types):
    if response.status_code != 200:
        raise OpenRouterError(response.status_code)
    mime = response.headers.get("content-type", "").split(";", 1)[0].lower()
    if mime not in types:
        raise ValueError("Unexpected OpenRouter audio response type")


class OpenRouterSTT(BatchSTT):
    def __init__(self, settings, *, client_factory=None, **kwargs):
        super().__init__(**kwargs)
        self.settings = settings
        self.http = _AudioClient(settings, client_factory)

    async def _open(self):
        self.http.get()

    async def _transcribe(self, pcm):
        validate_pcm(pcm)
        if not pcm:
            return ""
        if len(pcm) > self.queue.max_utterance_bytes:
            raise ValueError("STT audio exceeds configured limit")
        body = {
            "model": self.settings.model,
            "response_format": "json",
            "input_audio": {
                "data": base64.b64encode(pcm_to_wav(pcm)).decode("ascii"),
                "format": "wav",
            },
        }
        if self.settings.language:
            body["language"] = self.settings.language
        async with asyncio.timeout(self.settings.timeout):
            async with self.http.get().stream(
                "POST",
                BASE_URL + "/audio/transcriptions",
                json=body,
                headers=self.http.headers,
            ) as response:
                _check_response(response, {"application/json"})
                raw = bytearray()
                async for chunk in response.aiter_bytes(65536):
                    if len(raw) + len(chunk) > self.settings.max_response_bytes:
                        raise ValueError("STT response exceeds configured limit")
                    raw.extend(chunk)
        data = json.loads(raw)
        if not isinstance(data, dict) or not isinstance(data.get("text"), str):
            raise ValueError("Invalid OpenRouter transcript response")
        if len(data["text"]) > self.queue.max_final_chars:
            raise ValueError("STT transcript exceeds configured limit")
        return data["text"].strip()

    async def _close(self):
        await self.http.close()


class OpenRouterTTS(BaseTTS):
    def __init__(self, settings, *, client_factory=None, decoder=None):
        self.settings = settings
        self.decoder = decoder or (
            resample_pcm_stream
            if settings.native_pcm_resampler and settings.response_format == "pcm"
            else decode_audio_stream
        )
        self.http = _AudioClient(settings, client_factory)

    async def synthesize(self, text):
        async with aclosing(self.synthesize_stream(text)) as stream:
            return b"".join([chunk async for chunk in stream])

    async def synthesize_stream(self, text):
        text = text.strip()
        if not text:
            return
        if len(text) > self.settings.max_text_chars:
            raise ValueError("TTS text exceeds configured limit")
        if self.settings.expand_byn:
            text = pronounce_byn(text)
            if len(text) > self.settings.max_text_chars:
                raise ValueError("TTS spoken text exceeds configured limit")
        body = {
            "model": self.settings.model,
            "input": text,
            "response_format": self.settings.response_format,
        }
        if self.settings.voice:
            body["voice"] = self.settings.voice
        deadline = asyncio.get_running_loop().time() + self.settings.stream_timeout
        async with AsyncExitStack() as stack:
            async with deadline_timeout(deadline):
                response = await stack.enter_async_context(
                    self.http.get().stream(
                        "POST",
                        BASE_URL + "/audio/speech",
                        json=body,
                        headers=self.http.headers,
                    )
                )
            pcm_input = self.settings.response_format == "pcm"
            _check_response(
                response,
                {"audio/pcm"}
                if pcm_input
                else {"audio/mpeg", "audio/mp3", "application/octet-stream"},
            )
            if pcm_input:
                # Raw audio has no WAV header. Fail closed rather than play
                # unverified sample rate/channel metadata at the wrong speed.
                fields = response.headers.get_list("content-type")
                parts = fields[0].lower().split(";")
                parameters = [p.strip().split("=", 1) for p in parts[1:]]
                if (
                    len(fields) != 1
                    or any(len(p) != 2 for p in parameters)
                    or len(parameters) != 2
                    or dict(parameters) != {"rate": "24000", "channels": "1"}
                ):
                    raise ValueError("Unsupported raw TTS audio format")

            async def encoded():
                received = 0
                async for chunk in response.aiter_bytes(1024):
                    received += len(chunk)
                    if received > self.settings.max_mp3_bytes:
                        raise ValueError("TTS audio exceeds configured limit")
                    for offset in range(0, len(chunk), 65536):
                        yield chunk[offset : offset + 65536]
                if pcm_input and received % 2:
                    raise ValueError("Incomplete raw TTS PCM sample")

            received = 0
            async with aclosing(encoded()) as source:
                async with aclosing(
                    self.decoder(
                        source,
                        max_input_bytes=self.settings.max_mp3_bytes,
                        max_output_bytes=self.settings.max_pcm_bytes,
                        timeout=self.settings.stream_timeout,
                        **({"input_pcm_rate": 24000} if pcm_input else {}),
                    )
                ) as decoded:
                    while True:
                        async with deadline_timeout(deadline):
                            try:
                                pcm = await anext(decoded)
                            except StopAsyncIteration:
                                break
                        validate_pcm(pcm)
                        received += len(pcm)
                        if received > self.settings.max_pcm_bytes or len(pcm) > 65536:
                            raise ValueError("TTS PCM exceeds configured limit")
                        if pcm:
                            yield pcm
            if not received:
                raise ValueError("TTS returned empty audio")

    async def close(self):
        await self.http.close()
