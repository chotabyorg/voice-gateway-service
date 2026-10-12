"""Call ownership, typed boundary facts, barge-in and cancellation regressions."""

import asyncio
from pathlib import Path

import pytest

from tests.media.media_fakes import (
    FakeConversationPort,
    FakeSTT,
    FakeTTS,
    FakeTransport,
    pcm,
)
from voice_gateway.events import CallEndReason, PlaybackState
from voice_gateway.media.call import CallState, MediaCall, MediaSettings
from voice_gateway.media.interfaces import TransportIdleTimeout


async def start_call(*, transport=None, stt=None, tts=None, port=None, settings=None, **kwargs):
    transport = transport or FakeTransport()
    stt = stt or FakeSTT()
    tts = tts or FakeTTS()
    port = port or FakeConversationPort(response=None)
    call = MediaCall(transport, stt, tts, port, settings=settings or MediaSettings(), **kwargs)
    task = asyncio.create_task(call.run())
    if hasattr(stt, "started"):
        await asyncio.wait_for(stt.started.wait(), 1)
    return call, task, transport, stt, tts, port


async def until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_final_utterance_authorized_speech_and_truthful_playback_lifecycle():
    port = FakeConversationPort(response="Test response")
    call, task, transport, stt, tts, _ = await start_call(
        port=port,
        settings=MediaSettings(turn_silence_seconds=0.01),
    )
    await stt.emit_final("Hello from the caller")
    await until(lambda: any(item.state == PlaybackState.COMPLETED for item in port.playback))
    await transport.incoming.put(None)
    await asyncio.wait_for(task, 1)

    assert len(port.started) == 1 and len(port.utterances) == 1 and len(port.ended) == 1
    assert port.utterances[0].text == "Hello from the caller"
    assert tts.texts == ["Test response"] and b"".join(transport.sent) == tts.audio
    assert [item.state for item in port.playback] == [
        PlaybackState.STARTED,
        PlaybackState.COMPLETED,
    ]
    completed = port.playback[-1]
    assert completed.transport_accepted_bytes == completed.generated_bytes == len(tts.audio)
    assert call.state == CallState.CLOSED and transport.closed and stt.stopped and tts.closed
    assert port.ended[0].reason == CallEndReason.DISCONNECTED
    with pytest.raises(RuntimeError, match="once"):
        await call.run()


@pytest.mark.asyncio
async def test_oversized_speech_failure_does_not_reuse_previous_playback_bytes():
    port = FakeConversationPort(response=None)
    call, task, transport, _, _, _ = await start_call(
        port=port, settings=MediaSettings(max_tts_chars=8)
    )
    await call.speak("request-valid", "Valid")
    with pytest.raises(ValueError, match="configured limit"):
        await call.speak("request-too-long", "123456789")
    failure = port.playback[-1]
    assert failure.state == PlaybackState.FAILED
    assert failure.generated_bytes == failure.transport_accepted_bytes == 0
    await transport.incoming.put(None)
    await task


@pytest.mark.asyncio
async def test_transport_idle_timeout_is_reported_as_call_timeout():
    class IdleTransport(FakeTransport):
        async def read_audio_chunk(self):
            raise TransportIdleTimeout("inactivity")

    port = FakeConversationPort(response=None)
    _, task, _, _, _, _ = await start_call(transport=IdleTransport(), port=port)
    await asyncio.wait_for(task, 1)
    assert port.ended[0].reason == CallEndReason.TIMEOUT


@pytest.mark.asyncio
async def test_barge_in_cancels_playback_and_never_reports_completion():
    port = FakeConversationPort(response=None)
    transport, stt, tts = FakeTransport(), FakeSTT(), FakeTTS(pcm(2000, 1000))
    call, task, transport, stt, tts, _ = await start_call(
        transport=transport,
        stt=stt,
        tts=tts,
        port=port,
    )
    output = asyncio.create_task(call.speak("request-1", "Long response"))
    await asyncio.wait_for(transport.wrote.wait(), 1)
    leadin = pcm(100)
    await call.feed_audio(leadin)
    for _ in range(5):
        await call.feed_audio(pcm(3000))
    assert await asyncio.wait_for(output, 1) is False
    assert stt.chunks == [leadin] + [pcm(3000)] * 5
    assert 0 < sum(map(len, transport.sent)) < len(tts.audio)
    assert transport.clear_count == 1
    assert PlaybackState.INTERRUPTED in [item.state for item in port.playback]
    assert PlaybackState.COMPLETED not in [item.state for item in port.playback]
    interrupted = next(item for item in port.playback if item.state == PlaybackState.INTERRUPTED)
    assert interrupted.transport_accepted_bytes < interrupted.generated_bytes
    await call.feed_audio(pcm(1000))  # No suppression after caller interruption.
    assert stt.chunks[-1] == pcm(1000)
    await transport.incoming.put(None)
    await asyncio.wait_for(task, 1)


@pytest.mark.asyncio
async def test_barge_in_during_synthesis_cancels_provider_work():
    transport, stt, tts, port = (
        FakeTransport(),
        FakeSTT(),
        FakeTTS(),
        FakeConversationPort(response=None),
    )
    synthesizing, cancelled = asyncio.Event(), asyncio.Event()

    async def synthesize(text):
        synthesizing.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    tts.synthesize = synthesize
    call, task, *_ = await start_call(transport=transport, stt=stt, tts=tts, port=port)
    output = asyncio.create_task(call.speak("request-2", "Pending speech"))
    await asyncio.wait_for(synthesizing.wait(), 1)
    for _ in range(5):
        await call.feed_audio(pcm(3000))
    assert await asyncio.wait_for(output, 1) is False
    assert cancelled.is_set() and transport.sent == []
    assert PlaybackState.NOT_STARTED in [item.state for item in port.playback]
    await transport.incoming.put(None)
    await task


@pytest.mark.asyncio
async def test_disconnect_during_output_reports_unknown_or_interrupted_not_completed():
    transport, stt, tts, port = (
        FakeTransport(),
        FakeSTT(),
        FakeTTS(pcm(2000, 1000)),
        FakeConversationPort(),
    )
    call, task, *_ = await start_call(transport=transport, stt=stt, tts=tts, port=port)
    output = asyncio.create_task(call.speak("request-3", "Long response"))
    await asyncio.wait_for(transport.wrote.wait(), 1)
    await transport.incoming.put(None)
    await asyncio.wait_for(task, 1)
    assert await output is False
    assert PlaybackState.COMPLETED not in [item.state for item in port.playback]
    assert any(
        item.state in (PlaybackState.UNKNOWN, PlaybackState.INTERRUPTED) for item in port.playback
    )
    delivered = list(transport.sent)
    await stt.emit_final("late transcript")
    await asyncio.sleep(0)
    assert port.utterances == [] and transport.sent == delivered


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["prepare", "stt_start", "read", "timeout", "cancel"])
async def test_call_lifecycle_closes_owned_resources_on_failure(failure):
    transport, stt, tts = FakeTransport(), FakeSTT(), FakeTTS()

    async def fail(*args, **kwargs):
        raise ConnectionError("Injected failure")

    if failure == "prepare":
        transport.prepare = fail
    elif failure == "stt_start":
        stt.start_stream = fail
    elif failure == "read":
        transport.read_audio_chunk = fail
    call = MediaCall(
        transport,
        stt,
        tts,
        FakeConversationPort(response=None),
        settings=MediaSettings(max_call_seconds=0.02 if failure == "timeout" else 1),
    )
    task = asyncio.create_task(call.run())
    if failure == "cancel":
        await asyncio.wait_for(stt.started.wait(), 1)
        task.cancel()
    expected = (
        asyncio.CancelledError
        if failure == "cancel"
        else TimeoutError
        if failure == "timeout"
        else ConnectionError
    )
    with pytest.raises(expected):
        await task
    assert transport.closed and stt.stopped and tts.closed
    assert call.state == CallState.CLOSED and call.utterances.closed


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_skip_later_resources():
    transport, stt, tts, events = FakeTransport(), FakeSTT(), FakeTTS(), []

    async def fail():
        raise ConnectionError("Close failed")

    tts.close = fail
    call = MediaCall(
        transport,
        stt,
        tts,
        FakeConversationPort(response=None),
        emit=lambda event, **values: events.append((event, values)),
    )
    await transport.incoming.put(None)
    await call.run()
    assert transport.closed and stt.stopped
    assert ("media_cleanup_error", {"resource": "tts", "error_type": "ConnectionError"}) in events


@pytest.mark.asyncio
@pytest.mark.parametrize("resource", ["output", "transport", "stt", "tts"])
async def test_resource_self_cancellation_does_not_skip_remaining_cleanup(resource):
    transport, stt, tts = FakeTransport(), FakeSTT(), FakeTTS()
    call = MediaCall(transport, stt, tts, FakeConversationPort(response=None))

    async def cancel_resource(**kwargs):
        raise asyncio.CancelledError("resource cancelled")

    if resource == "output":
        call.interrupt = cancel_resource
    elif resource == "transport":
        transport.close = cancel_resource
    elif resource == "stt":
        stt.stop_stream = cancel_resource
    else:
        tts.close = cancel_resource
    await transport.incoming.put(None)
    with pytest.raises(asyncio.CancelledError, match="resource cancelled"):
        await call.run()
    assert call.state == CallState.CLOSED
    assert resource == "tts" or tts.closed
    assert resource == "stt" or stt.stopped
    assert resource == "transport" or transport.closed


@pytest.mark.asyncio
async def test_lifecycle_observer_failure_does_not_leak_resources():
    transport, stt, tts = FakeTransport(), FakeSTT(), FakeTTS()

    def fail(event, **values):
        raise ValueError("Broken observer")

    call = MediaCall(transport, stt, tts, FakeConversationPort(response=None), emit=fail)
    await transport.incoming.put(None)
    await call.run()
    assert call.state == CallState.CLOSED and transport.closed and stt.stopped and tts.closed


def test_no_spool_or_local_canonical_store_is_present():
    root = Path(__file__).resolve().parents[2]
    package = root / "voice_gateway"
    assert not list(package.rglob("*spool*"))
    assert not any(path.suffix in {".sqlite", ".sqlite3", ".db"} for path in root.rglob("*"))
    source = "\n".join(path.read_text(encoding="utf-8") for path in package.rglob("*.py"))
    assert "sqlite3" not in source and "RecordingSpool" not in source
