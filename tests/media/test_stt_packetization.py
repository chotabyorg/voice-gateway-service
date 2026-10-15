"""Offline acoustic scenarios: identical PCM must survive PBX repacketization."""

import asyncio
import struct

import pytest

from tests.media.media_fakes import pcm
from voice_gateway.media.batch_stt import BatchSTT, BatchSTTSettings
from voice_gateway.media.vad import VADSettings


class CapturingSTT(BatchSTT):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.audio = []

    async def _transcribe(self, audio):
        self.audio.append(audio)
        return "offline fixture"


async def transcribe(audio, packet_bytes):
    stt = CapturingSTT(vad=VADSettings(silence_end_ms=100))
    await stt.start_stream()
    for start in range(0, len(audio), packet_bytes):
        await stt.feed_audio(audio[start : start + packet_bytes])
    await stt.stop_stream()
    return stt.audio


@pytest.mark.parametrize("packet_bytes", [2, 126, 320, 962, 4096])
@pytest.mark.parametrize("scenario", ["quiet_tail", "click", "pause", "partial_tail"])
async def test_stt_segmentation_is_independent_of_packet_boundaries(packet_bytes, scenario):
    # Sparse energy inside each 20 ms window makes per-sample VAD incorrect.
    voiced = (struct.pack("<h", 1600) * 40 + pcm(0, 15)) * 5
    fixtures = {
        "quiet_tail": pcm(100, 40) + voiced + pcm(350, 80) + pcm(0, 100),
        "click": pcm(100, 100) + pcm(1600, 20) + pcm(100, 200),
        "pause": voiced + pcm(0, 40) + voiced + pcm(0, 100),
        "partial_tail": voiced + pcm(650, 7),
    }
    audio = fixtures[scenario]
    expected = await transcribe(audio, 320)
    assert await transcribe(audio, packet_bytes) == expected
    if scenario == "click":
        assert expected == []
    else:
        assert len(expected) == 1 and expected[0] == audio


async def test_restart_drops_old_partial_frame():
    stt = CapturingSTT()
    await stt.start_stream()
    await stt.feed_audio(pcm(1200, 7))
    await stt.stop_stream()
    await stt.start_stream()
    await stt.feed_audio(pcm(1500, 100))
    await stt.stop_stream()
    assert stt.audio == [pcm(1500, 100)]


async def test_cancellation_resistant_stt_cannot_deliver_after_stop():
    release, entered = asyncio.Event(), asyncio.Event()
    finals = []

    class Resistant(BatchSTT):
        async def _transcribe(self, audio):
            entered.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    pass
            return "late fixture"

    async def final(text):
        finals.append(text)

    stt = Resistant(queue=BatchSTTSettings(shutdown_timeout=0.04))
    await stt.start_stream(on_final=final)
    await stt.feed_audio(pcm(1200, 100) + pcm(0, 500))
    await entered.wait()
    try:
        await asyncio.wait_for(stt.stop_stream(), 0.3)
    finally:
        release.set()
        await asyncio.gather(*tuple(stt._active_tasks))
    assert finals == []


async def test_acoustic_byte_limit_is_enforced_before_large_vad_buffer_grows():
    stt = CapturingSTT(queue=BatchSTTSettings(max_utterance_bytes=1600))
    await stt.start_stream()
    await stt.feed_audio(pcm(1200, 100))
    with pytest.raises(RuntimeError, match="buffer"):
        await stt.feed_audio(pcm(1200, 100))
    assert len(b"".join(stt.vad.buffer)) <= 1600
    await stt.stop_stream()


async def test_thirty_minutes_of_synthetic_audio_keep_buffers_and_tasks_bounded():
    finals = 0

    class CountingSTT(BatchSTT):
        async def _transcribe(self, audio):
            assert len(audio) == 16000
            return "fixture"

    async def final(text):
        nonlocal finals
        finals += 1

    stt = CountingSTT(vad=VADSettings(silence_end_ms=500))
    await stt.start_stream(on_final=final)
    # Accelerated sample time, no recording files or wall-clock sleep. Each
    # one-second utterance includes 200 ms preroll, 300 ms voice and 500 ms silence.
    audio = pcm(100, 200) + pcm(1200, 300) + pcm(0, 500)
    for _ in range(1800):
        for offset in range(0, len(audio), 4096):
            await stt.feed_audio(audio[offset : offset + 4096])
            assert stt.vad.buffer_bytes <= 16000
            assert len(stt._pending_frame) < 320
        await asyncio.sleep(0)
        assert len(stt._active_tasks) <= 2
    await stt.stop_stream()
    assert finals == 1800 and not stt._active_tasks
    assert not stt.vad.buffer and not stt.vad.preroll and not stt._pending_frame
