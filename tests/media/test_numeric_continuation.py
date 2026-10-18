"""A dictated phone number must not be answered as a finished request."""

import asyncio
import time

import pytest

from voice_gateway.media.utterances import (
    UtteranceCoordinator,
    complete_phone_dictation,
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


@pytest.mark.parametrize(
    "text",
    ["+375173820015", "+375 17 382 00 15", "+375 (17) 382-00-15.", " +375 17 382-00-15! "],
)
async def test_complete_explicit_phone_uses_normal_continuation_window(text):
    assert complete_phone_dictation(text)
    coordinator = UtteranceCoordinator(None)
    started = time.monotonic()
    await coordinator.submit(text)
    try:
        assert 0.8 <= coordinator.deadline - started <= 1.0
    finally:
        await coordinator.close()


@pytest.mark.parametrize(
    "text",
    [
        "+375 17 382 00",
        "+3751738200156",
        "375173820015",
        "+375 (17 382-00-15",
        "+375 )17( 382-00-15",
        "плюс три семь пять",
        "+375 17 382 00 15 добавочный 2",
        "номер +375173820015",
        "+12025550112",
        "+375173820015 или +375173820016",
        "+375１７３８２００１５",
    ],
)
def test_fragments_other_formats_and_corrections_do_not_take_fast_path(text):
    assert not complete_phone_dictation(text)


async def test_speech_and_following_final_still_postpone_complete_number():
    seen = []
    delivered = asyncio.Event()

    async def callback(text):
        seen.append(text)
        delivered.set()

    coordinator = UtteranceCoordinator(
        callback,
        silence_seconds=0.08,
        continuation_seconds=0.03,
        numeric_continuation_seconds=0.12,
    )
    await coordinator.submit("+375173820015")
    coordinator.speech()
    try:
        await asyncio.sleep(0.04)
        assert not seen
        await coordinator.submit("пятнадцать, исправьте на шестнадцать")
        await asyncio.wait_for(delivered.wait(), 0.4)
        assert seen == ["+375173820015 пятнадцать, исправьте на шестнадцать"]
    finally:
        await coordinator.close()


async def test_close_fences_pending_complete_number():
    seen = []

    async def callback(text):
        seen.append(text)

    coordinator = UtteranceCoordinator(callback)
    await coordinator.submit("+375173820015")
    await coordinator.close()
    assert seen == [] and not coordinator.pending and coordinator.task.done()
