"""Bounded cross-call cache for synthesized telephone speech.

Repeated short phrases skip the provider round trip entirely: audio is
available as soon as the text arrives. The store holds only raw PCM bytes
keyed by a hash of the voice descriptor plus the exact text; failures and
empty results are never stored. Voice descriptors differ per
provider/model/voice/role/format, so a voice change cannot serve stale
audio. Bounds cap entries, text length and total bytes with LRU eviction.
Provider errors propagate unchanged and no text is logged.
"""

from __future__ import annotations

import hashlib
from collections import OrderedDict


class TTSCache:
    """Small LRU byte store shared by wrapper instances across calls."""

    def __init__(self, *, max_entries: int = 64, max_bytes: int = 2 * 1024 * 1024) -> None:
        if not 1 <= max_entries <= 4096 or not 1024 <= max_bytes <= 64 * 1024 * 1024:
            raise ValueError("Invalid TTS cache bounds")
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._items: OrderedDict[str, bytes] = OrderedDict()
        self._total_bytes = 0

    def get(self, key: str) -> bytes | None:
        try:
            audio = self._items.pop(key)
        except KeyError:
            return None
        self._items[key] = audio
        return audio

    def put(self, key: str, audio: bytes) -> None:
        if not audio or len(audio) > self._max_bytes:
            return
        previous = self._items.pop(key, None)
        if previous is not None:
            self._total_bytes -= len(previous)
        self._items[key] = audio
        self._total_bytes += len(audio)
        while len(self._items) > self._max_entries or self._total_bytes > self._max_bytes:
            _, dropped = self._items.popitem(last=False)
            self._total_bytes -= len(dropped)


def _cache_key(voice_key: str, text: str) -> str:
    digest = hashlib.sha256()
    digest.update(voice_key.encode("utf-8"))
    digest.update(b"\x00")
    digest.update(text.encode("utf-8"))
    return digest.hexdigest()


class CachingTTS:
    """Wraps any TTS adapter with an exact-text cache; streaming included.

    synthesize_stream is always available: adapters without native streaming
    are served from a single synthesize() call split into bounded chunks.
    """

    def __init__(
        self, inner, *, cache: TTSCache, voice_key: str, max_text_chars: int = 200
    ) -> None:
        if not voice_key or not voice_key.isascii() or not 1 <= max_text_chars <= 2000:
            raise ValueError("Invalid TTS cache configuration")
        self._inner = inner
        self._cache = cache
        self._voice_key = voice_key
        self._max_text_chars = max_text_chars

    def _lookup(self, text: str) -> tuple[str | None, bytes | None]:
        if not isinstance(text, str) or not text or len(text) > self._max_text_chars:
            return None, None
        key = _cache_key(self._voice_key, text)
        return key, self._cache.get(key)

    async def synthesize(self, text: str) -> bytes:
        key, cached = self._lookup(text)
        if cached is not None:
            return cached
        audio = await self._inner.synthesize(text)
        if key is not None and audio:
            self._cache.put(key, audio)
        return audio

    async def synthesize_stream(self, text: str):
        key, cached = self._lookup(text)
        if cached is not None:
            for offset in range(0, len(cached), 65536):
                yield cached[offset : offset + 65536]
            return
        stream = getattr(self._inner, "synthesize_stream", None)
        if stream is None:
            audio = await self.synthesize(text)
            for offset in range(0, len(audio), 65536):
                yield audio[offset : offset + 65536]
            return
        chunks = bytearray()
        async for piece in stream(text):
            chunks.extend(piece)
            yield piece
        if key is not None and chunks:
            self._cache.put(key, bytes(chunks))

    async def close(self) -> None:
        await self._inner.close()
