"""A delayed final must not cut off newer classified caller speech."""

import asyncio
from types import SimpleNamespace

import pytest

from voice_gateway.media import utterances


REQUEST = "Запишите меня завтра на стрижку. Меня зовут Артем."
CONTACT = "Телефон плюс триста семьдесят пять двадцать девять сто один двадцать два тридцать три"


@pytest.mark.parametrize("speech_before_final", [False, True])
async def test_delayed_contact_stays_with_request(monkeypatch, speech_before_final):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(utterances, "time", SimpleNamespace(monotonic=lambda: clock.now))
    seen = []
    delivered = asyncio.Event()

    async def callback(text):
        seen.append(text)
        delivered.set()

    coordinator = utterances.UtteranceCoordinator(callback, silence_seconds=0.3)
    try:
        coordinator.speech(ended_at=99.8)
        if speech_before_final:
            coordinator.speech(ended_at=100.5)
        await coordinator.submit(REQUEST, speech_ended_at=99.8)
        if not speech_before_final:
            coordinator.speech(ended_at=100.5)
        # Acoustic silence has elapsed, but the newer contact final is pending.
        clock.now = 101.0
        await asyncio.sleep(0)
        assert seen == []
        await coordinator.submit(CONTACT, speech_ended_at=100.5)
        clock.now = 104.0
        coordinator.changed.set()
        await asyncio.wait_for(delivered.wait(), 0.2)
        assert seen == [REQUEST + " " + CONTACT]
        assert coordinator.dispatched_speech_at is None
    finally:
        await coordinator.close()


async def test_missing_followup_final_releases_request_at_bounded_deadline(monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(utterances, "time", SimpleNamespace(monotonic=lambda: clock.now))
    seen = []
    delivered = asyncio.Event()

    async def callback(text):
        seen.append(text)
        delivered.set()

    coordinator = utterances.UtteranceCoordinator(callback, silence_seconds=0.3)
    try:
        coordinator.speech(ended_at=99.8)
        await coordinator.submit(REQUEST, speech_ended_at=99.8)
        coordinator.speech(ended_at=100.5)
        clock.now = 101.0
        await asyncio.sleep(0)
        assert seen == []
        clock.now = 103.1
        coordinator.changed.set()
        await asyncio.wait_for(delivered.wait(), 0.2)
        assert seen == [REQUEST]
    finally:
        await coordinator.close()


@pytest.mark.parametrize("coverage", [None, 100.5])
async def test_quiet_request_has_only_bounded_final_arrival_grace(monkeypatch, coverage):
    clock = SimpleNamespace(now=101.0)
    monkeypatch.setattr(utterances, "time", SimpleNamespace(monotonic=lambda: clock.now))
    delivered = asyncio.Event()

    async def callback(text):
        assert text == REQUEST
        delivered.set()

    coordinator = utterances.UtteranceCoordinator(callback, silence_seconds=0.3)
    try:
        coordinator.speech(ended_at=100.5)
        await coordinator.submit(REQUEST, speech_ended_at=coverage)
        clock.now = 101.31
        coordinator.changed.set()
        await asyncio.wait_for(delivered.wait(), 0.2)
    finally:
        await coordinator.close()


async def test_speech_starting_after_quick_final_joins_before_dispatch(monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(utterances, "time", SimpleNamespace(monotonic=lambda: clock.now))
    seen = []
    delivered = asyncio.Event()

    async def callback(text):
        seen.append(text)
        delivered.set()

    coordinator = utterances.UtteranceCoordinator(callback, silence_seconds=0.5)
    try:
        coordinator.speech(ended_at=99.0)
        await coordinator.submit(REQUEST, speech_ended_at=99.0)
        await asyncio.sleep(0)
        assert seen == []
        clock.now = 100.2
        coordinator.speech(ended_at=100.2)
        clock.now = 100.8
        coordinator.changed.set()
        await asyncio.sleep(0)
        assert seen == []
        await coordinator.submit(CONTACT, speech_ended_at=100.2)
        clock.now = 104.0
        coordinator.changed.set()
        await asyncio.wait_for(delivered.wait(), 0.2)
        assert seen == [REQUEST + " " + CONTACT]
    finally:
        await coordinator.close()


async def test_close_discards_request_waiting_for_recognition(monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(utterances, "time", SimpleNamespace(monotonic=lambda: clock.now))
    seen = []

    async def callback(text):
        seen.append(text)

    coordinator = utterances.UtteranceCoordinator(callback, silence_seconds=0.3)
    coordinator.speech(ended_at=99.8)
    await coordinator.submit(REQUEST, speech_ended_at=99.8)
    coordinator.speech(ended_at=100.5)
    await asyncio.sleep(0)
    await coordinator.close()
    clock.now = 104.0
    await coordinator.submit(CONTACT, speech_ended_at=100.5)
    assert seen == []
    assert coordinator.pending == []
    assert coordinator.pending_chars == 0
