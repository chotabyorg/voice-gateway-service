"""Corrections and long admitted finals retain time to finish phone dictation."""

import time

import pytest

from voice_gateway.media.utterances import UtteranceCoordinator, complete_phone_dictation


@pytest.mark.parametrize("prefix", ["Ошибся", "Ошиблась", "Я ошибся"])
async def test_correction_prefix_keeps_numeric_window(prefix):
    text = prefix + ", плюс 375 29 111 22 33"
    assert not complete_phone_dictation(text)
    coordinator = UtteranceCoordinator(None)
    try:
        started = time.monotonic()
        await coordinator.submit(text)
        assert 2.4 <= coordinator.deadline - started <= 2.6
        assert coordinator.pending == [text]
    finally:
        await coordinator.close()


@pytest.mark.parametrize("length", [257, 16000])
async def test_long_permitted_final_with_incomplete_phone_keeps_numeric_window(length):
    suffix = " +375 29"
    text = "А" * (length - len(suffix)) + suffix
    assert not complete_phone_dictation(text)
    coordinator = UtteranceCoordinator(None)
    try:
        started = time.monotonic()
        await coordinator.submit(text)
        assert 2.4 <= coordinator.deadline - started <= 2.6
        assert coordinator.pending == [text]
    finally:
        await coordinator.close()
