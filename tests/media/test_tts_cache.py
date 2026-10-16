"""Bounded TTS cache: hits skip the provider, misses and failures pass through."""

import pytest

from tests.media.media_fakes import FakeTTS
from voice_gateway.media.providers.cached import CachingTTS, TTSCache


def pcm16(seconds, amplitude=2000):
    import struct

    count = int(seconds * 8000)
    return struct.pack("<" + "h" * count, *([amplitude] * count))


class StreamingFake(FakeTTS):
    def __init__(self, audio=None, fail=False):
        super().__init__(audio=audio)
        self.fail = fail
        self.streams = 0

    async def synthesize_stream(self, text):
        self.streams += 1
        if self.fail:
            raise RuntimeError("provider unavailable")
        for offset in range(0, len(self.audio), 1000):
            yield self.audio[offset : offset + 1000]


class FailingFake(FakeTTS):
    async def synthesize(self, text):
        self.texts.append(text)
        raise RuntimeError("provider unavailable")


def cached(inner=None, **kwargs):
    kwargs.setdefault("cache", TTSCache())
    kwargs.setdefault("voice_key", "speechkit:livetts:sofia:support")
    return CachingTTS(FakeTTS() if inner is None else inner, **kwargs)


async def test_second_identical_request_skips_provider():
    tts = cached()
    first = await tts.synthesize("Да.")
    second = await tts.synthesize("Да.")
    assert first == second
    assert tts._inner.texts == ["Да."]


async def test_voice_key_separates_entries():
    cache = TTSCache()
    first = CachingTTS(FakeTTS(audio=pcm16(0.05, 1000)), cache=cache, voice_key="voice-a")
    second = CachingTTS(FakeTTS(audio=pcm16(0.05, 2000)), cache=cache, voice_key="voice-b")
    assert await first.synthesize("Да.") != await second.synthesize("Да.")
    assert await first.synthesize("Да.") == pcm16(0.05, 1000)


async def test_provider_failure_is_never_cached():
    tts = cached(FailingFake())
    with pytest.raises(RuntimeError, match="provider unavailable"):
        await tts.synthesize("Да.")
    with pytest.raises(RuntimeError, match="provider unavailable"):
        await tts.synthesize("Да.")
    assert tts._inner.texts == ["Да.", "Да."]


async def test_empty_audio_is_not_cached():
    tts = cached(FakeTTS(audio=b""))
    assert await tts.synthesize("Да.") == b""
    assert await tts.synthesize("Да.") == b""
    assert tts._inner.texts == ["Да.", "Да."]


async def test_long_text_is_not_cached():
    tts = cached(max_text_chars=4)
    assert await tts.synthesize("коротко но длинно") == tts._inner.audio
    assert await tts.synthesize("коротко но длинно") == tts._inner.audio
    assert tts._inner.texts == ["коротко но длинно", "коротко но длинно"]


async def test_oldest_entry_is_evicted_first():
    cache = TTSCache(max_entries=2)
    tts = CachingTTS(FakeTTS(), cache=cache, voice_key="v")
    await tts.synthesize("один")
    await tts.synthesize("два")
    await tts.synthesize("три")
    assert await tts.synthesize("два") == tts._inner.audio
    assert tts._inner.texts.count("два") == 1
    assert await tts.synthesize("один") == tts._inner.audio
    assert tts._inner.texts.count("один") == 2


async def test_oversized_audio_is_not_stored():
    cache = TTSCache(max_bytes=1024)
    tts = CachingTTS(FakeTTS(audio=pcm16(0.5)), cache=cache, voice_key="v")
    assert await tts.synthesize("Да.") == pcm16(0.5)
    assert await tts.synthesize("Да.") == pcm16(0.5)
    assert tts._inner.texts == ["Да.", "Да."]


async def test_stream_serves_cached_audio_without_provider():
    inner = StreamingFake()
    tts = cached(inner)
    first = b"".join([part async for part in tts.synthesize_stream("Да.")])
    second = b"".join([part async for part in tts.synthesize_stream("Да.")])
    assert first == second == inner.audio
    assert inner.streams == 1
    assert all(
        len(part) <= 65536 for part in [second[i : i + 65536] for i in range(0, len(second), 65536)]
    )


async def test_stream_without_native_support_falls_back_to_single_request():
    tts = cached()
    assert not hasattr(tts._inner, "synthesize_stream")
    heard = b"".join([part async for part in tts.synthesize_stream("Да.")])
    assert heard == tts._inner.audio
    assert tts._inner.texts == ["Да."]
    replay = b"".join([part async for part in tts.synthesize_stream("Да.")])
    assert replay == heard
    assert tts._inner.texts == ["Да."]


async def test_stream_failure_is_not_cached():
    tts = cached(StreamingFake(fail=True))
    with pytest.raises(RuntimeError, match="provider unavailable"):
        b"".join([part async for part in tts.synthesize_stream("Да.")])
    with pytest.raises(RuntimeError, match="provider unavailable"):
        b"".join([part async for part in tts.synthesize_stream("Да.")])
    assert tts._inner.streams == 2


async def test_close_reaches_inner_adapter():
    tts = cached()
    await tts.close()
    assert tts._inner.closed is True


def test_invalid_configuration_is_rejected():
    with pytest.raises(ValueError):
        TTSCache(max_entries=0)
    with pytest.raises(ValueError):
        TTSCache(max_bytes=100)
    with pytest.raises(ValueError):
        CachingTTS(FakeTTS(), cache=TTSCache(), voice_key="")
