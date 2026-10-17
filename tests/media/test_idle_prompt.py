"""An idle caller is asked once, through an authorized synthetic turn."""

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
