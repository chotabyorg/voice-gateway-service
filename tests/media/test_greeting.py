"""Leading greeting suppression never silences authorized speech."""

import pytest

from tests.media.media_fakes import FakeTTS, pcm
from voice_gateway.media.providers.greeting import (
    GreetingStripTTS,
    strip_leading_greeting,
    strip_stock_formulas,
    strip_trailing_offer,
)


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
    with pytest.raises(ValueError):
        strip_trailing_offer(None)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Запись на завтра в десять. Чем я могу вам помочь?", "Запись на завтра в десять."),
        ("Хорошо, записала. Могу ли я ещё чем-то помочь?", "Хорошо, записала."),
        ("Салон работает с девяти. Что вас интересует?", "Салон работает с девяти."),
        ("Поняла. Есть ли ещё вопросы?", "Поняла."),
        ("До связи. Обращайтесь!", "До связи."),
        ("Чем могу помочь?", "Чем могу помочь?"),
        ("Чем я могу вам помочь?", "Чем я могу вам помочь?"),
        ("Помочь с выбором скорости?", "Помочь с выбором скорости?"),
        (
            "Запись подтверждена, остальное уточню завтра.",
            "Запись подтверждена, остальное уточню завтра.",
        ),
    ],
)
def test_trailing_offer_removed_only_as_last_sentence(text, expected):
    assert strip_trailing_offer(text) == expected


async def test_wrapper_applies_both_formulas_before_provider():
    inner = FakeTTS()
    tts = GreetingStripTTS(inner)
    await tts.synthesize("Здравствуйте! Запись на завтра. Чем могу помочь?")
    assert inner.texts == ["Запись на завтра."]


def test_stock_formulas_compose_in_one_pass():
    assert (
        strip_stock_formulas("Здравствуйте! Запись на завтра. Чем могу помочь?")
        == "Запись на завтра."
    )
    assert strip_stock_formulas("Слушаю вас.") == "Слушаю вас."
    assert strip_stock_formulas("Чем могу помочь?") == "Чем могу помочь?"


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
