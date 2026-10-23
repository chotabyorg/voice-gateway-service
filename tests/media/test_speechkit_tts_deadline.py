"""Deadline checks on buffered SpeechKit PCM, using only a synthetic RPC."""

import asyncio
from contextlib import aclosing

import pytest
from yandex.cloud.ai.tts.v3 import tts_pb2 as p

from voice_gateway.media.playback import play_pcm_stream
from voice_gateway.media.providers.speechkit import SpeechKitTTS, SpeechKitTTSSettings


class BufferedRPC:
    def __init__(self, data):
        self.data = data
        self.cancelled = False

    async def write(self, request):
        pass

    async def done_writing(self):
        pass

    def cancel(self):
        self.cancelled = True

    async def __aiter__(self):
        yield p.StreamSynthesisResponse(
            audio_chunk=p.AudioChunk(data=self.data),
            chunk_type=p.StreamSynthesisResponse.AUDIO_ONLY,
        )


class Connection:
    def __init__(self, rpc):
        self.rpc = rpc

    def StreamSynthesis(self, **kwargs):
        return self.rpc

    async def close(self):
        pass


def make_tts(data, timeout=10):
    rpc = BufferedRPC(data)
    connection = Connection(rpc)
    tts = SpeechKitTTS(
        SpeechKitTTSSettings("fixture", stream_timeout=timeout),
        connection_factory=lambda: (connection, connection),
    )
    return tts, rpc


@pytest.mark.parametrize("size,consumed", [(65538, 1), (131074, 1), (131074, 2)])
async def test_expiry_before_buffered_slice_does_not_emit_or_cancel_consumer(
    monkeypatch, size, consumed
):
    loop = asyncio.get_running_loop()
    now = [loop.time()]
    monkeypatch.setattr(loop, "time", lambda: now[0])
    tts, rpc = make_tts(b"\1\0" * (size // 2))
    async with aclosing(tts.synthesize_stream("Test")) as stream:
        for _ in range(consumed):
            assert len(await anext(stream)) == 65536
        now[0] += 11
        # The generator must not retain a timeout context around consumer work.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="^SpeechKit TTS stream failed$"):
            await anext(stream)
    assert rpc.cancelled and not tts._rpcs
    await tts.close()


async def test_paced_consumer_cannot_request_more_buffered_pcm_after_deadline(monkeypatch):
    loop = asyncio.get_running_loop()
    now = [loop.time()]
    monkeypatch.setattr(loop, "time", lambda: now[0])
    tts, rpc = make_tts(b"\1\0" * 65536, timeout=2)
    accepted = []

    async def write(data):
        accepted.append(data)

    async def pace(delay):
        now[0] += delay
        await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="^SpeechKit TTS stream failed$"):
        await play_pcm_stream(
            tts.synthesize_stream("Test"), write, clock=lambda: now[0], sleep=pace
        )
    # Playback owns a partial frame at the boundary; no second provider slice arrived.
    assert sum(map(len, accepted)) == 65536 // 320 * 320
    assert rpc.cancelled and not tts._rpcs
    await tts.close()


@pytest.mark.parametrize("size", [2, 65536, 65538, 131074])
async def test_all_buffered_audio_unchanged_before_deadline(size):
    data = b"\1\0" * (size // 2)
    tts, rpc = make_tts(data)
    assert await tts.synthesize("Test") == data
    assert rpc.cancelled and not tts._rpcs
    await tts.close()
