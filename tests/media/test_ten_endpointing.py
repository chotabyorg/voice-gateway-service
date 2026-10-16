import asyncio

import numpy as np
import pytest

from tests.media.media_fakes import FakeConversationPort, FakeTransport, FakeTTS, pcm
from tests.media.test_routing import config
from tests.media.test_stt_packetization import CapturingSTT
from voice_gateway.live import build_live_runtime
from voice_gateway.media.call import CallState, MediaCall
from voice_gateway.media.interruption import InterruptionDetector
from voice_gateway.media.vad import EnergyVAD, VADSettings
from voice_gateway.settings import Settings
from voice_gateway.speech_activity import TenSpeech


class InspectingNative:
    def __init__(self):
        self.frames = []

    def process(self, frame):
        assert frame.dtype == np.int16 and frame.shape == (160,)
        self.frames.append(frame.copy())
        return 0.9, int(np.max(np.abs(frame.astype(np.int32))) >= 700)


def test_ten_continuous_resampling_and_both_hops_without_overflow():
    classifier = TenSpeech()
    native = classifier.detector = InspectingNative()
    assert classifier(pcm(32767))
    assert len(native.frames) == 2
    assert np.concatenate(native.frames).tolist() == [16383] + [32767] * 319
    assert classifier(pcm(-32768))
    assert native.frames[2][0] == -1 and native.frames[2][1] == -32768
    assert np.all(native.frames[3] == -32768)
    assert not classifier(b"")
    for invalid in (bytes(3), bytes(322)):
        with pytest.raises(ValueError):
            classifier(invalid)
    classifier(bytes(126))  # A partial shutdown frame is padded only for VAD.
    assert len(native.frames) == 6


@pytest.mark.parametrize("packet_bytes", [126, 320, 4096])
async def test_ten_endpoint_keeps_pcm_and_dispatches_while_background_continues(packet_bytes):
    classifier = TenSpeech()
    native = classifier.detector = InspectingNative()
    stt = CapturingSTT(speech_classifier=classifier, vad=VADSettings(min_speech_ms=120))
    port = FakeConversationPort(None)
    events = []
    stt.emit = lambda event, **values: events.append((event, values))
    call = MediaCall(FakeTransport(), stt, FakeTTS(), port)
    call.state = CallState.ACTIVE
    audio = pcm(300, 300) + pcm(1000, 700) + pcm(300, 3000)
    await stt.start_stream(on_final=call._final)
    try:
        for offset in range(0, len(audio), packet_bytes):
            await call.feed_audio(audio[offset : offset + packet_bytes])
        await asyncio.sleep(0)
        assert stt.audio == [pcm(300, 200) + pcm(1000, 700) + pcm(300, 500)]
        assert len(native.frames) == len(audio) // 160
        endpoint = next(values for event, values in events if event == "stt_endpoint")
        assert endpoint == dict(
            audio_ms=1400, speech_ms=700, silence_ms=500, endpoint_reason="silence"
        )
        clock = call.utterances.last_speech
        await call.feed_audio(pcm(300, 600))
        assert call.utterances.last_speech == clock
        async with asyncio.timeout(1):
            while not port.utterances:
                await asyncio.sleep(0.01)
        assert len(port.utterances) == 1
    finally:
        await call.utterances.close()
        await stt.abort_stream()


def test_ten_barge_in_feeds_quiet_frames_but_keeps_duration_guard():
    classifier = TenSpeech()
    native = classifier.detector = InspectingNative()
    detector = InterruptionDetector(classifier=classifier)
    for _ in range(25):
        assert detector.feed(pcm(300)) is None
    assert len(native.frames) == 50
    for _ in range(4):
        assert detector.feed(pcm(1000)) is None
    event = detector.feed(pcm(1000))
    assert event is not None and b"".join(event.chunks).endswith(pcm(1000, 100))


def test_native_ten_silence_and_stationary_noise_do_not_open_an_utterance():
    classifier = TenSpeech()
    vad = EnergyVAD(VADSettings(min_speech_ms=120), classifier=classifier)
    rng = np.random.default_rng(23)
    frames = [bytes(320)] * 50
    frames += [rng.integers(-500, 501, 160, dtype=np.int16).tobytes() for _ in range(150)]
    # TEN may emit one startup-positive frame. The real 120 ms speech guard
    # must reject it without scheduling STT or leaving an acoustic buffer open.
    assert not any(vad.feed(frame) for frame in frames)
    assert not vad.has_speech and vad.buffer_bytes == 0


async def test_live_ten_opt_in_and_independent_native_state(monkeypatch):
    monkeypatch.setattr("voice_gateway.live.shutil.which", lambda name: "ffmpeg")
    runtime = build_live_runtime(Settings(mode="live_audiosocket"), config(vad="ten"))
    first, second = runtime.stt_factory(), runtime.stt_factory()
    assert isinstance(first.vad.classifier, TenSpeech)
    assert first.vad.classifier is not second.vad.classifier
    assert runtime.speech_classifier_factory() is not first.vad.classifier
    await runtime.stop()
    monkeypatch.setattr(
        "voice_gateway.live.importlib.util.find_spec",
        lambda name: None if name == "ten_vad" else True,
    )
    with pytest.raises(ValueError, match="TEN"):
        build_live_runtime(Settings(mode="live_audiosocket"), config(vad="ten"))
