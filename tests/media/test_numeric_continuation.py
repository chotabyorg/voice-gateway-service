"""A dictated phone number must not be answered as a finished request."""

import time

import pytest

from voice_gateway.media.utterances import (
    UtteranceCoordinator,
    likely_unfinished,
    numeric_dictation,
)


@pytest.mark.parametrize(
    "text",
    [
        "плюс три семь",
        "пять пять пять",
        "+375 29 111 22 33",
        "восемь девятьсот",
        "код 29",
        "two nine",
    ],
)
def test_numeric_dictation_fragments_are_unfinished(text):
    assert numeric_dictation(text) is True
    assert likely_unfinished(text) is True


@pytest.mark.parametrize(
    "text",
    [
        "Сколько стоит маникюр?",
        "Запишите меня на тринадцатое июня",
        "тридцать пять рублей",
        "Меня зовут Анна",
    ],
)
def test_normal_speech_is_not_numeric_dictation(text):
    assert numeric_dictation(text) is False


@pytest.mark.asyncio
async def test_numeric_final_waits_the_longer_window():
    seen = []

    async def callback(text):
        seen.append(text)

    coordinator = UtteranceCoordinator(
        callback, silence_seconds=0.3, continuation_seconds=0.9, numeric_continuation_seconds=2.5
    )
    await coordinator.submit("пять пять пять", speech_ended_at=time.monotonic())
    remaining = coordinator.deadline - time.monotonic()
    await coordinator.close()
    assert 2.0 <= remaining <= 2.6


@pytest.mark.asyncio
async def test_plain_final_has_no_extra_window():
    async def callback(text):
        return None

    coordinator = UtteranceCoordinator(callback, silence_seconds=0.3, continuation_seconds=0.9)
    now = time.monotonic()
    await coordinator.submit("Сколько стоит маникюр?", speech_ended_at=now)
    assert coordinator.deadline - now <= 0.01
    await coordinator.close()


def test_numeric_window_cannot_be_shorter_than_continuation():
    with pytest.raises(ValueError):
        UtteranceCoordinator(
            lambda text: None, continuation_seconds=2.0, numeric_continuation_seconds=1.0
        )
