import asyncio
import random
import struct

import pytest

from tests.media.media_fakes import FakeConversationPort, FakeTransport, FakeTTS, pcm
from tests.media.test_stt_packetization import CapturingSTT
from tests.media.test_routing import config
from voice_gateway.live import build_live_runtime
from voice_gateway.media.call import CallState, MediaCall, MediaSettings
from voice_gateway.media.interruption import InterruptionDetector
from voice_gateway.media.vad import VADSettings
from voice_gateway.settings import Settings
from voice_gateway.speech_activity import WebRTCSpeech


@pytest.mark.parametrize("packet_bytes", [126, 320, 4096])
async def test_classified_end_preserves_audio_despite_louder_background(packet_bytes):
    stt = CapturingSTT(
        speech_classifier=lambda frame: frame.startswith(struct.pack("<h", 700)),
        vad=VADSettings(silence_end_ms=100),
    )
    audio = pcm(1200, 200) + pcm(700, 200) + pcm(1200, 300)
    await stt.start_stream()
    for offset in range(0, len(audio), packet_bytes):
        await stt.feed_audio(audio[offset : offset + packet_bytes])
    await asyncio.sleep(0)
    assert stt.audio == [audio[:8000]]  # 200 ms preroll, speech, 100 ms tail.
    await stt.stop_stream()
    assert len(stt.audio) == 1


async def test_final_reaches_conversation_before_hangup_while_background_continues():
    stt = CapturingSTT(
        speech_classifier=lambda frame: frame.startswith(struct.pack("<h", 700)),
        vad=VADSettings(silence_end_ms=40),
    )
    port = FakeConversationPort(None)
    call = MediaCall(
        FakeTransport(),
        stt,
        FakeTTS(),
        port,
        settings=MediaSettings(turn_silence_seconds=0.03),
    )
    call.state = CallState.ACTIVE
    await stt.start_stream(on_final=call._final)
    try:
        for _ in range(5):
            await call.feed_audio(pcm(700))
        async with asyncio.timeout(1):
            while not port.utterances:
                await call.feed_audio(pcm(1200))
                await asyncio.sleep(0.01)
        assert len(port.utterances) == 1
        assert call.state == CallState.ACTIVE
        assert not port.ended
    finally:
        await call.utterances.close()
        await stt.stop_stream()


def test_classified_interruption_rejects_loud_noise_but_keeps_quiet_speech_preroll():
    detector = InterruptionDetector(
        classifier=lambda frame: frame.startswith(struct.pack("<h", 700))
    )
    for _ in range(50):
        assert detector.feed(pcm(1200)) is None
    for _ in range(4):
        assert detector.feed(pcm(700)) is None
    event = detector.feed(pcm(700))
    assert event is not None and b"".join(event.chunks).endswith(pcm(700, 100))


async def test_native_classifier_does_not_open_stt_for_stationary_noise():
    rng = random.Random(23)
    stt = CapturingSTT(speech_classifier=WebRTCSpeech(), vad=VADSettings(min_speech_ms=120))
    await stt.start_stream()
    for _ in range(150):
        await stt.feed_audio(struct.pack("<160h", *[rng.randint(-1000, 1000) for _ in range(160)]))
    await stt.stop_stream()
    assert stt.audio == []


def test_native_classifier_rejects_invalid_frames_and_accepts_partial_tail():
    detector = WebRTCSpeech()
    assert not detector(b"")
    assert not detector(bytes(126))
    with pytest.raises(ValueError):
        detector(bytes(322))
    with pytest.raises(ValueError):
        detector(bytes(3))


async def test_live_classifier_is_explicit_and_owned_per_call(monkeypatch):
    monkeypatch.setattr("voice_gateway.live.shutil.which", lambda name: "ffmpeg")
    runtime = build_live_runtime(Settings(mode="live_audiosocket"), config(vad="webrtc"))
    first, second = runtime.stt_factory(), runtime.stt_factory()
    assert first.uses_speech_classifier and second.uses_speech_classifier
    assert first.vad.classifier is not second.vad.classifier
    assert runtime.speech_classifier_factory() is not first.vad.classifier
    await runtime.stop()
    monkeypatch.setattr(
        "voice_gateway.live.importlib.util.find_spec",
        lambda module: None if module == "webrtcvad" else True,
    )
    with pytest.raises(ValueError, match="WebRTC"):
        build_live_runtime(Settings(mode="live_audiosocket"), config(vad="webrtc"))
