"""Long requests should not add a numeric hold after a complete phone suffix."""

import asyncio
from types import SimpleNamespace

import pytest

from voice_gateway.media import utterances


REQUEST = (
    "Запишите меня завтра двадцатого октября в час дня на мужскую стрижку к Светлане. "
    "Меня зовут Артем."
)
PHONE = "Телефон плюс триста семьдесят пять двадцать девять сто двадцать три сорок два ноль четыре"


@pytest.mark.parametrize("split", [False, True])
async def test_complete_suffix_keeps_existing_phone_hold_without_rewriting(monkeypatch, split):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(utterances, "time", SimpleNamespace(monotonic=lambda: clock.now))
    delivered = asyncio.Event()
    seen = []

    async def callback(text):
        seen.append(text)
        delivered.set()

    coordinator = utterances.UtteranceCoordinator(callback, silence_seconds=0.3)
    try:
        coordinator.speech(ended_at=99.9)
        texts = [REQUEST, PHONE] if split else [REQUEST + " " + PHONE]
        for text in texts:
            await coordinator.submit(text, speech_ended_at=99.9)
        assert coordinator.deadline == 100.9
        clock.now = 101.0
        coordinator.changed.set()
        await asyncio.wait_for(delivered.wait(), 0.2)
        assert seen == [REQUEST + " " + PHONE]
    finally:
        await coordinator.close()


@pytest.mark.parametrize(
    "text",
    [
        REQUEST + " Телефон плюс триста семьдесят пять двадцать девять сто двадцать три",
        REQUEST + " " + PHONE + " пять",
        REQUEST + " " + PHONE + " или пять",
        REQUEST + " " + PHONE + " добавочный два",
        "Ой, ошибся. " + REQUEST + " " + PHONE,
        REQUEST + " Нет, " + PHONE,
        REQUEST + " " + PHONE + ", последняя цифра другая",
        REQUEST + " +375 29 123 42 03 или +375 29 123 42 04",
        REQUEST + " плюс семь девятьсот одиннадцать сто двадцать три сорок два ноль четыре",
        "А" * 513 + " " + PHONE,
        REQUEST + " \x00 " + PHONE,
    ],
)
async def test_ambiguity_incomplete_suffix_and_bounds_keep_long_hold(monkeypatch, text):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(utterances, "time", SimpleNamespace(monotonic=lambda: clock.now))
    coordinator = utterances.UtteranceCoordinator(None)
    try:
        await coordinator.submit(text, speech_ended_at=99.9)
        assert coordinator.deadline == 102.5
        assert coordinator.pending == [text]
    finally:
        await coordinator.close()


@pytest.mark.parametrize("speech_before_final", [False, True])
async def test_newer_correction_still_fences_complete_request(monkeypatch, speech_before_final):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(utterances, "time", SimpleNamespace(monotonic=lambda: clock.now))
    delivered = asyncio.Event()
    seen = []

    async def callback(text):
        seen.append(text)
        delivered.set()

    coordinator = utterances.UtteranceCoordinator(callback, silence_seconds=0.3)
    try:
        coordinator.speech(ended_at=99.9)
        if speech_before_final:
            coordinator.speech(ended_at=100.5)
        await coordinator.submit(REQUEST + " " + PHONE, speech_ended_at=99.9)
        if not speech_before_final:
            coordinator.speech(ended_at=100.5)
        clock.now = 101.0
        await asyncio.sleep(0)
        assert seen == []
        await coordinator.submit("Ой, ошибся, последняя цифра три", speech_ended_at=100.5)
        clock.now = 101.5
        coordinator.changed.set()
        await asyncio.sleep(0)
        assert seen == []
        clock.now = 104.0
        coordinator.changed.set()
        await asyncio.wait_for(delivered.wait(), 0.2)
        assert seen == [REQUEST + " " + PHONE + " Ой, ошибся, последняя цифра три"]
    finally:
        await coordinator.close()
