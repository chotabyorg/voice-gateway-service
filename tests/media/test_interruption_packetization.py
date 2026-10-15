"""Barge-in depends on the audio, not the PBX packet boundaries."""

import asyncio

import pytest

from tests.media.media_fakes import FakeTTS, pcm
from tests.media.test_media_hardening import active_call
from voice_gateway.events import PlaybackState
from voice_gateway.media.call import MediaSettings
from voice_gateway.media.interruption import InterruptionDetector, InterruptionSettings


@pytest.mark.parametrize("packet_bytes", [2, 126, 320, 960, 4096])
def test_a_loud_click_in_a_large_packet_is_not_sustained_speech(packet_bytes):
    audio = pcm(0, 120) + pcm(3000, 20) + pcm(0, 100)
    detector = InterruptionDetector()
    for offset in range(0, len(audio), packet_bytes):
        assert detector.feed(audio[offset : offset + packet_bytes]) is None


@pytest.mark.parametrize("packet_bytes", [2, 126, 320, 960, 4096])
def test_quiet_leadin_does_not_hide_speech_and_packet_tail_is_retained(packet_bytes):
    audio = pcm(0, 120) + pcm(700, 120)
    detector = InterruptionDetector()
    for offset in range(0, len(audio), packet_bytes):
        packet = audio[offset : offset + packet_bytes]
        event = detector.feed(packet)
        if event is not None:
            # The rest of the triggering packet belongs to the caller too.
            retained = b"".join(event.chunks) + audio[offset + len(packet) :]
            assert retained == audio
            break
    else:
        pytest.fail("A sustained caller utterance did not interrupt playback")


def test_reset_drops_partial_analysis_frame_from_previous_playback():
    detector = InterruptionDetector()
    assert detector.feed(pcm(3000, 10)) is None
    detector.reset()
    assert detector.feed(pcm(1000, 90)) is None
    event = detector.feed(pcm(1000, 10))
    assert event is not None
    assert b"".join(event.chunks) == pcm(1000, 100)


def test_guard_window_is_evaluated_at_each_audio_frame_end():
    detector = InterruptionDetector(
        InterruptionSettings(
            guarded=True, required_ms=100, calibration_seconds=0.1, min_playback_seconds=0.35
        )
    )
    # Only the last 60 ms ends at/after the 350 ms guard boundary.
    assert detector.feed(pcm(1500, 200), elapsed=0.4) is None
    event = detector.feed(pcm(1500, 40), elapsed=0.44)
    assert event is not None
    assert b"".join(event.chunks) == pcm(1500, 100)


async def test_call_passes_entire_triggering_packet_to_stt():
    call = active_call(FakeTTS(pcm(2000, 2000)), settings=MediaSettings(smooth_barge_in=False))
    output = asyncio.create_task(call.speak("packet", "Fixture"))
    try:
        await asyncio.wait_for(call.transport.wrote.wait(), 1)
        audio = pcm(0, 120) + pcm(700, 120)
        await call.feed_audio(audio)
        assert b"".join(call.stt.chunks) == audio
        assert await asyncio.wait_for(output, 0.3) is False
        assert call.conversation_port.playback[-1].state == PlaybackState.INTERRUPTED
    finally:
        await call.interrupt(reason="test_cleanup")
        await output
