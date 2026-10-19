"""New acoustic speech must fence a phone final while its recognition is pending."""

import asyncio
from types import SimpleNamespace

import pytest

from voice_gateway.media import utterances


PHONE = "плюс триста семьдесят пять двадцать девять сто один двадцать два тридцать три"


@pytest.mark.parametrize("speech_before_final", [False, True])
async def test_new_speech_waits_for_delayed_correction_final(monkeypatch, speech_before_final):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(utterances, "time", SimpleNamespace(monotonic=lambda: clock.now))
    seen = []
    delivered = asyncio.Event()

    async def callback(text):
        seen.append(text)
        delivered.set()

    coordinator = utterances.UtteranceCoordinator(callback, silence_seconds=0.3)
    try:
        coordinator.speech(ended_at=99.9)
        if speech_before_final:
            coordinator.speech(ended_at=100.5)
        await coordinator.submit(PHONE, speech_ended_at=99.9)
        if not speech_before_final:
            coordinator.speech(ended_at=100.5)
        # The short hold and VAD silence have expired, but the correction's STT
        # final has not arrived. Do not dispatch the earlier complete number.
        clock.now = 101.0
        await asyncio.sleep(0)
        assert seen == []
        await coordinator.submit("Ой, ошибся", speech_ended_at=100.5)
        clock.now = 103.6
        coordinator.changed.set()
        await asyncio.wait_for(delivered.wait(), 0.2)
        assert seen == [PHONE + " Ой, ошибся"]
    finally:
        await coordinator.close()


async def test_unrecognized_followup_uses_bounded_numeric_hold(monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(utterances, "time", SimpleNamespace(monotonic=lambda: clock.now))
    seen = []
    delivered = asyncio.Event()

    async def callback(text):
        seen.append(text)
        delivered.set()

    coordinator = utterances.UtteranceCoordinator(callback, silence_seconds=0.3)
    try:
        coordinator.speech(ended_at=99.9)
        await coordinator.submit(PHONE, speech_ended_at=99.9)
        coordinator.speech(ended_at=100.5)
        clock.now = 101.0
        await asyncio.sleep(0)
        assert seen == []
        # A missing final must not strand buffered input forever.
        clock.now = 103.1
        coordinator.changed.set()
        await asyncio.wait_for(delivered.wait(), 0.2)
        assert seen == [PHONE]
    finally:
        await coordinator.close()


async def test_quiet_complete_phone_keeps_short_hold(monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(utterances, "time", SimpleNamespace(monotonic=lambda: clock.now))
    delivered = asyncio.Event()

    async def callback(text):
        assert text == PHONE
        delivered.set()

    coordinator = utterances.UtteranceCoordinator(callback, silence_seconds=0.3)
    try:
        coordinator.speech(ended_at=99.9)
        await coordinator.submit(PHONE, speech_ended_at=99.9)
        clock.now = 101.0
        await asyncio.wait_for(delivered.wait(), 0.2)
    finally:
        await coordinator.close()
