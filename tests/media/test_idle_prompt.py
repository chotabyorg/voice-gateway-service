"""An idle caller is asked once, through an authorized synthetic turn."""

import asyncio
import time

import pytest

from tests.media.media_fakes import FakeConversationPort, FakeSTT, FakeTTS, FakeTransport
from voice_gateway.media.call import IDLE_MARKER, CallState, MediaCall, MediaSettings


@pytest.mark.asyncio
async def test_idle_prompt_admits_one_synthetic_turn():
    port = FakeConversationPort(None)
    call = MediaCall(
        FakeTransport(),
        FakeSTT(),
        FakeTTS(),
        port,
        settings=MediaSettings(idle_prompt_seconds=0.05, max_idle_prompts=1),
    )
    call.state = CallState.ACTIVE
    await call._idle_prompts()
    assert [event.text for event in port.utterances] == [IDLE_MARKER]
    assert call._idle_nudges == 1


def test_idle_prompt_is_off_by_default():
    call = MediaCall(FakeTransport(), FakeSTT(), FakeTTS(), FakeConversationPort())
    assert call.settings.idle_prompt_seconds == 0
    assert call._idle_task is None


@pytest.mark.asyncio
async def test_hard_silence_ends_the_call_after_the_last_nudge():
    call = MediaCall(
        FakeTransport(),
        FakeSTT(),
        FakeTTS(),
        FakeConversationPort(None),
        settings=MediaSettings(idle_prompt_seconds=0.05, max_idle_prompts=1),
    )
    call.state = CallState.ACTIVE
    await call._idle_prompts()
    assert call._idle_nudges == 1
    assert call._end_call.is_set()


@pytest.mark.asyncio
async def test_idle_clock_starts_when_the_agent_stops_speaking():
    """A short answer must not be followed by a nudge a moment later."""

    class SlowTTS(FakeTTS):
        async def synthesize(self, text):
            await asyncio.sleep(0.2)
            self.texts.append(text)
            return self.audio

    call = MediaCall(FakeTransport(), FakeSTT(), SlowTTS(), FakeConversationPort(None))
    call.state = CallState.ACTIVE
    started = time.monotonic()
    assert await call.speak("turn", "Запись на завтра.") is True
    assert call._last_prompt_at >= started + 0.15
