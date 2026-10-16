"""Leading greeting suppression never silences authorized speech."""

import pytest

from tests.media.media_fakes import FakeTTS, pcm
from voice_gateway.media.providers.greeting import GreetingStripTTS, strip_leading_greeting


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Здравствуйте! Чем могу помочь?", "Чем могу помочь?"),
        ("здравствуйте, слушаю вас.", "слушаю вас."),
        ("Добрый день! Салон на связи.", "Салон на связи."),
        ("Добрый вечер, чем помочь?", "чем помочь?"),
        ("Привет! Как дела?", "Как дела?"),
        ("Приветствую вас в салоне.", "вас в салоне."),
        ("Слушаю вас.", "Слушаю вас."),
        ("Да, конечно.", "Да, конечно."),
        ("Здравствуйте", "Здравствуйте"),
        ("", ""),
    ],
)
def test_strip_removes_one_greeting_formula_or_keeps_text(text, expected):
    assert strip_leading_greeting(text) == expected


def test_strip_rejects_non_text():
    with pytest.raises(ValueError):
        strip_leading_greeting(None)


async def test_wrapper_speaks_stripped_text_to_provider():
    inner = FakeTTS()
    tts = GreetingStripTTS(inner)
    assert await tts.synthesize("Здравствуйте! Чем могу помочь?") == inner.audio
    assert inner.texts == ["Чем могу помочь?"]


async def test_wrapper_stream_strips_once_and_chunks():
    inner = FakeTTS(audio=pcm(1200, 40))

    async def stream(text):
        inner.texts.append(text)
        for offset in range(0, len(inner.audio), 1000):
            yield inner.audio[offset : offset + 1000]

    inner.synthesize_stream = stream
    tts = GreetingStripTTS(inner)
    heard = b"".join([part async for part in tts.synthesize_stream("Добрый день! Салон.")])
    assert heard == inner.audio
    assert inner.texts == ["Салон."]


async def test_wrapper_stream_falls_back_without_native_support():
    inner = FakeTTS()
    assert not hasattr(inner, "synthesize_stream")
    tts = GreetingStripTTS(inner)
    heard = b"".join([part async for part in tts.synthesize_stream("Привет! Да.")])
    assert heard == inner.audio
    assert inner.texts == ["Да."]


async def test_close_reaches_inner_adapter():
    tts = GreetingStripTTS(FakeTTS())
    await tts.close()
    assert tts._inner.closed is True
