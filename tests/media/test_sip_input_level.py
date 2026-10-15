import struct
from types import SimpleNamespace as NS

import pytest

from tests.media.test_sip import settings
from tests.media.test_stt_packetization import CapturingSTT
from voice_gateway.media.audio import amplify_pcm
from voice_gateway.sip import SIPTransport


def pcm(value, frames=1):
    return struct.pack("<h", value) * (160 * frames)


def test_gain_preserves_polarity_silence_and_saturates_instead_of_wrapping():
    source = struct.pack("<7h", -32768, -3000, -40, 0, 40, 3000, 32767)
    assert amplify_pcm(source, 1) is source
    assert struct.unpack("<7h", amplify_pcm(source, 16)) == (
        -32768,
        -32768,
        -640,
        0,
        640,
        32767,
        32767,
    )
    assert amplify_pcm(bytes(320), 32) == bytes(320)


@pytest.mark.parametrize("gain", [0, 33, float("nan"), float("inf")])
def test_gain_is_bounded(gain):
    with pytest.raises(ValueError):
        settings(input_gain=gain)
    with pytest.raises(ValueError):
        amplify_pcm(bytes(320), gain)


@pytest.mark.parametrize("gain,expected_requests", [(1, 0), (16, 1)])
async def test_quiet_sip_input_reaches_stt_without_triggering_on_idle_noise(
    gain, expected_requests
):
    transport = SIPTransport(NS(settings=settings(input_gain=gain)), None)
    stt = CapturingSTT()
    await stt.start_stream()
    # G.711 idle floor stays below VAD even with the selected gain. A quiet
    # caller must open VAD, retain the phrase, and close it on trailing noise.
    for frame in [pcm(8)] * 40 + [pcm(40)] * 15 + [pcm(8)] * 30:
        transport.audio.receive(frame)
        await stt.feed_audio(await transport.read_audio_chunk())
    await stt.stop_stream()
    assert len(stt.audio) == expected_requests
    if stt.audio:
        assert pcm(640, 15) in stt.audio[0]
    await transport.write_audio_chunk(pcm(200))
    assert transport.audio.request(320) == pcm(200)  # Outbound remains unchanged.
    await transport.close()
    assert transport.input_rms_max == 40
    assert transport.input_bytes == 85 * 320


async def test_empty_provider_result_is_observable_without_transcript_or_audio():
    events = []

    class EmptySTT(CapturingSTT):
        async def _transcribe(self, audio):
            return ""

    stt = EmptySTT(emit=lambda event, **fields: events.append((event, fields)))
    await stt.start_stream()
    await stt.feed_audio(pcm(1200, 5) + pcm(0, 25))
    await stt.stop_stream()
    empty = [fields for event, fields in events if event == "stt_empty_result"]
    assert empty == [{"audio_ms": 600}]
    assert not any(event == "stt_final" for event, _ in events)
