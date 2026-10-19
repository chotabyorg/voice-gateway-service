"""Bounded acoustic hints for the actual spoken-number STT format."""

import asyncio
import time

import pytest

from voice_gateway.media.utterances import UtteranceCoordinator, complete_phone_dictation


@pytest.mark.parametrize(
    "text",
    [
        "плюс три семь пять два девять один один один два два три три",
        "плюс триста семьдесят пять двадцать девять сто один двадцать два тридцать три",
        "Меня зовут Артём. Телефон плюс триста семьдесят пять двадцать девять сто один двадцать два тридцать три.",
        "Арте\u0308м, плюс триста семьдесят пять двадцать девять сто один двадцать два тридцать три",
        "Мой номер +375 29 111 22 33",
        "+375 двадцать девять сто один двадцать два тридцать три",
        "плюс 375 29 001 22 33",
        "плюс триста семьдесят пять двадцать девять ноль один ноль двадцать два тридцать три",
    ],
)
async def test_complete_spoken_phone_with_optional_name_uses_normal_hold(text):
    assert complete_phone_dictation(text)
    coordinator = UtteranceCoordinator(None)
    try:
        started = time.monotonic()
        await coordinator.submit(text)
        assert 0.8 <= coordinator.deadline - started <= 1.0
    finally:
        await coordinator.close()


@pytest.mark.parametrize(
    "text",
    [
        "плюс триста семьдесят пять двадцать девять сто один двадцать два",
        "плюс триста семьдесят пять двадцать девять сто один двадцать два тридцать три четыре",
        "Нет, плюс триста семьдесят пять двадцать девять сто один двадцать два тридцать три",
        "плюс триста семьдесят пять двадцать девять сто один двадцать два тридцать три, последняя цифра другая",
        "плюс триста семьдесят пять двадцать девять сто один двадцать два тридцать три добавочный два",
        "плюс триста семьдесят пять двадцать девять сто один двадцать два тридцать три или четыре",
        "плюс семь девятьсот одиннадцать сто один двадцать два тридцать три",
        "плюс триста семьдесят пять двадцать девять тысяча двадцать два тридцать три",
        "+375 2 9 111 22 33",
        "+375２９１１１２２３３",
        "+375 29 111 22 33, нет, последняя цифра четыре",
    ],
)
async def test_partial_ambiguous_other_country_and_correction_keep_long_hold(text):
    assert not complete_phone_dictation(text)
    coordinator = UtteranceCoordinator(None)
    try:
        started = time.monotonic()
        await coordinator.submit(text)
        assert 2.4 <= coordinator.deadline - started <= 2.6
    finally:
        await coordinator.close()


async def test_complete_number_is_recognized_across_pending_acoustic_finals():
    coordinator = UtteranceCoordinator(None)
    try:
        await coordinator.submit("Артём. Телефон плюс триста")
        started = time.monotonic()
        await coordinator.submit(
            "семьдесят пять двадцать девять сто один двадцать два тридцать три"
        )
        assert 0.8 <= coordinator.deadline - started <= 1.0
        assert len(coordinator.pending) == 2
    finally:
        await coordinator.close()


async def test_late_correction_extends_hold_and_preserves_original_finals():
    seen = []
    delivered = asyncio.Event()

    async def callback(text):
        seen.append(text)
        delivered.set()

    first = "плюс триста семьдесят пять двадцать девять сто один двадцать два тридцать три"
    correction = "нет, последняя цифра четыре"
    coordinator = UtteranceCoordinator(
        callback, silence_seconds=0.01, continuation_seconds=0.04, numeric_continuation_seconds=0.12
    )
    try:
        await coordinator.submit(first)
        await asyncio.sleep(0.01)
        await coordinator.submit(correction)
        await asyncio.sleep(0.05)
        assert seen == []
        await asyncio.wait_for(delivered.wait(), 0.3)
        assert seen == [first + " " + correction]
    finally:
        await coordinator.close()
