"""Provider-independent regressions for real media edge cases."""

import asyncio
import time
import uuid

import pytest

from tests.media.media_fakes import (
    FakeConversationPort,
    FakeSTT,
    FakeTTS,
    FakeTransport,
    FakeWriter,
    pcm,
    socket_frame,
)
from voice_gateway.events import CallEndReason, PlaybackState
from voice_gateway.media.audiosocket import AudioSocketTransport
from voice_gateway.media.batch_stt import BatchSTT
from voice_gateway.media.call import CallState, MediaCall, MediaSettings
from voice_gateway.media.playback import PlaybackControl, play_pcm_stream
from voice_gateway.media.vad import EnergyVAD, VADSettings


def active_call(tts=None, **kwargs):
    call = MediaCall(
        FakeTransport(), FakeSTT(), tts or FakeTTS(), FakeConversationPort(None), **kwargs
    )
    call.state = CallState.ACTIVE
    return call


async def test_closed_audiosocket_cannot_claim_accepted_audio():
    transport = AudioSocketTransport(asyncio.StreamReader(), FakeWriter())
    await transport.close()
    call = active_call()
    call.transport = transport
    with pytest.raises(ConnectionError):
        await call.speak("closed", "Fixture")
    fact = call.conversation_port.playback[-1]
    assert fact.state == PlaybackState.UNKNOWN and fact.transport_accepted_bytes == 0
    assert not transport._writer.frames


async def test_repeated_uuid_is_idempotent_but_identity_cannot_change():
    identity = uuid.uuid4()
    reader = asyncio.StreamReader()
    reader.feed_data(socket_frame(1, identity.bytes) * 2 + socket_frame(0x10, pcm()))
    transport = AudioSocketTransport(reader, FakeWriter())
    await transport.prepare()
    assert await transport.read_audio_chunk() == pcm()
    reader.feed_data(socket_frame(1, uuid.uuid4().bytes))
    with pytest.raises(ValueError, match="UUID changed"):
        await transport.read_audio_chunk()
    assert transport.call_id == str(identity)
    await transport.close()


async def test_peer_error_is_failed_call_not_normal_hangup():
    reader = asyncio.StreamReader()
    reader.feed_data(socket_frame(1, uuid.uuid4().bytes) + socket_frame(0xFF, b"\x01"))
    call = active_call()
    call.state = CallState.CREATED
    call.transport = AudioSocketTransport(reader, FakeWriter())
    with pytest.raises(ConnectionError):
        await call.run()
    assert call.conversation_port.ended[-1].reason == CallEndReason.FAILED


async def test_immediate_reply_survives_end_of_playback():
    call = active_call()
    await call.speak("reply", "Fixture")
    await call.feed_audio(pcm(1500, 100))
    assert call.stt.chunks == [pcm(1500, 100)]


async def test_resistant_synthesis_cannot_block_input_or_emit_late_audio():
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class Resistant(FakeTTS):
        async def synthesize(self, text):
            entered.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
            return pcm(2000)

    call = active_call(Resistant(), settings=MediaSettings(interrupt_timeout=0.01))
    output = asyncio.create_task(call.speak("old", "Fixture"))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(call.feed_audio(pcm(2000, 100)), 0.3)
        assert cancelled.is_set() and await asyncio.wait_for(output, 0.3) is False
        assert call.stt.chunks == [pcm(2000, 100)]
        with pytest.raises(RuntimeError, match="still stopping"):
            await call.speak("new", "Next")
    finally:
        release.set()
        await asyncio.gather(*tuple(call._retired_outputs), return_exceptions=True)
        await output
    assert call.transport.sent == []
    assert [f.state for f in call.conversation_port.playback] == [PlaybackState.NOT_STARTED]


async def test_cancel_during_first_transport_write_reports_unknown():
    entered = asyncio.Event()
    call = active_call(settings=MediaSettings(smooth_barge_in=False))

    async def write(chunk):
        entered.set()
        await asyncio.Future()

    call.transport.write_audio_chunk = write
    output = asyncio.create_task(call.speak("write", "Fixture"))
    await asyncio.wait_for(entered.wait(), 1)
    await call.interrupt()
    assert await output is False
    assert call.conversation_port.playback[-1].state == PlaybackState.UNKNOWN


async def test_stream_starts_before_synthesis_finishes_and_closes_on_barge_in():
    closed = asyncio.Event()

    class Streaming(FakeTTS):
        async def synthesize_stream(self, text):
            try:
                yield pcm(2000, 100)
                await asyncio.Future()
            finally:
                closed.set()

    call = active_call(Streaming(), settings=MediaSettings(smooth_barge_in=False))
    output = asyncio.create_task(call.speak("stream", "Fixture"))
    await asyncio.wait_for(call.transport.wrote.wait(), 1)
    await call.interrupt()
    assert await output is False and closed.is_set()
    assert call.conversation_port.playback[-1].state == PlaybackState.INTERRUPTED


async def test_stream_reframes_split_samples_and_rejects_incomplete_tail():
    sent = []

    async def source():
        for chunk in (b"\x01", b"\0" * 317, b"\0" * 323, b"\0"):
            yield chunk

    async def write(chunk):
        sent.append(chunk)

    result = await play_pcm_stream(source(), write)
    assert result.complete and [len(c) for c in sent] == [320, 320, 2]

    async def incomplete():
        yield b"\0"

    with pytest.raises(ValueError, match="complete PCM16"):
        await play_pcm_stream(incomplete(), write)


async def test_pacing_does_not_burst_after_provider_or_transport_stall():
    now, accepted, events = [0.0], [], []

    async def source():
        yield pcm(1000, 40)
        now[0] += 0.3  # Provider pauses before producing the next frames.
        yield pcm(1000, 40)

    async def write(chunk):
        if not accepted:
            now[0] += 0.1  # First transport write blocks.
        accepted.append(now[0])

    async def sleep(seconds):
        now[0] += seconds

    result = await play_pcm_stream(
        source(),
        write,
        clock=lambda: now[0],
        sleep=sleep,
        emit=lambda event, **values: events.append(event),
    )
    assert result.complete and len(accepted) == 4
    assert all(b - a >= 0.019 for a, b in zip(accepted, accepted[1:]))
    assert events == ["playback_pacing_stall", "playback_pacing_stall"]


def test_vad_hysteresis_keeps_quiet_endings_without_promoting_clicks():
    vad = EnergyVAD(VADSettings(threshold=500, release_ratio=0.6, silence_end_ms=100))
    assert not vad.feed(pcm(600, 100))
    assert not vad.feed(pcm(350, 200))
    assert vad.speech_ms == 300
    assert vad.feed(pcm(0, 100))
    vad.reset()
    assert not vad.feed(pcm(600, 20))
    assert not vad.feed(pcm(350, 100))
    assert not vad.has_speech


def test_gain_ramp_stops_and_cannot_resume_revoked_output():
    control = PlaybackControl()
    control.duck()
    control.apply(pcm(2000, 80))
    assert control.gain == pytest.approx(0.2)
    control.pause()
    control.apply(pcm(2000, 40))
    assert not control.ready.is_set()
    control.resume()
    control.apply(pcm(2000, 40))
    assert control.gain == 1
    control.stop()
    control.apply(pcm(2000, 40))
    control.resume()
    assert control.finished


async def test_first_audio_latency_uses_utterance_timestamp_once():
    events = []
    call = active_call(emit=lambda event, **values: events.append((event, values)))
    await call.speak("timed", "Fixture", speech_ended_at=time.monotonic() - 0.5)
    gaps = [v for e, v in events if e == "media_response_gap"]
    assert len(gaps) == 1 and gaps[0]["duration_ms"] >= 500
    await call.speak("untimed", "Fixture")
    assert len([e for e, _ in events if e == "media_response_gap"]) == 1


async def test_ordered_transcripts_keep_their_own_speech_timestamp():
    release = asyncio.Event()

    class STT(BatchSTT):
        async def _transcribe(self, audio):
            if audio == pcm(1000):
                await release.wait()
            return "fixture"

    stt = STT()
    timestamps = []

    async def final(text):
        timestamps.append(stt.final_speech_at)

    await stt.start_stream(on_final=final)
    first = asyncio.create_task(stt._transcribe_and_callback(pcm(1000), speech_ended_at=1))
    second = asyncio.create_task(stt._transcribe_and_callback(pcm(2000), first, 2))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)
    assert timestamps == [1, 2] and stt.final_speech_at is None
    await stt.stop_stream()
