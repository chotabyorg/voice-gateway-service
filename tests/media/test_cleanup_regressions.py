"""Real subprocess cleanup and transport uncertainty regressions."""

import asyncio
import sys

import pytest

from tests.media.media_fakes import FakeConversationPort, FakeSTT, FakeTTS, FakeTransport, pcm
from voice_gateway.events import PlaybackState
from voice_gateway.media.call import MediaCall, MediaSettings
from voice_gateway.media.providers.edge import decode_mp3


def executable(tmp_path, body):
    path = tmp_path / "fake-decoder"
    path.write_text(f"#!{sys.executable}\n{body}\n", encoding="utf-8")
    path.chmod(0o700)
    return (sys.executable, str(path))


@pytest.mark.asyncio
async def test_decoder_enforces_output_limit_and_reaps_child(tmp_path, children):
    marker = tmp_path / "pid"
    command = executable(
        tmp_path,
        (
            "import os,sys\n"
            f"open({str(marker)!r}, 'w').write(str(os.getpid()))\n"
            "sys.stdout.buffer.write(b'\\0\\0' * 100000)\n"
            "sys.stdout.flush()\n"
        ),
    )
    with pytest.raises(ValueError, match="limit"):
        await decode_mp3(b"fixture", executable=command, max_output_bytes=1024)
    assert children[-1].returncode is not None


@pytest.mark.asyncio
async def test_decoder_timeout_and_cancellation_reap_child(tmp_path, children):
    marker = tmp_path / "pid"
    command = executable(
        tmp_path,
        (f"import os,time\nopen({str(marker)!r}, 'w').write(str(os.getpid()))\ntime.sleep(30)\n"),
    )
    with pytest.raises(TimeoutError):
        await decode_mp3(b"fixture", executable=command, timeout=1)
    assert children[-1].returncode is not None

    marker.unlink()
    task = asyncio.create_task(decode_mp3(b"fixture", executable=command, timeout=30))
    async with asyncio.timeout(2):
        while not marker.exists():
            await asyncio.sleep(0.005)
    task.cancel("test cancellation")
    with pytest.raises(asyncio.CancelledError, match="test cancellation"):
        await task
    assert children[-1].returncode is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_after", [0, 1])
async def test_transport_failure_reports_unknown_even_if_acceptance_is_zero(fail_after):
    transport, stt, tts, port = (
        FakeTransport(),
        FakeSTT(),
        FakeTTS(pcm(1000, 40)),
        FakeConversationPort(None),
    )
    write = transport.write_audio_chunk

    async def fail_second(chunk):
        if len(transport.sent) >= fail_after:
            raise ConnectionError("transport failed")
        await write(chunk)

    transport.write_audio_chunk = fail_second
    call = MediaCall(transport, stt, tts, port)
    task = asyncio.create_task(call.run())
    await asyncio.wait_for(stt.started.wait(), 1)
    with pytest.raises(ConnectionError):
        await call.speak("partial", "Partly delivered")
    assert len(transport.sent) == fail_after
    assert PlaybackState.UNKNOWN in [item.state for item in port.playback]
    assert PlaybackState.COMPLETED not in [item.state for item in port.playback]
    unknown = next(item for item in port.playback if item.state == PlaybackState.UNKNOWN)
    assert unknown.transport_accepted_bytes == fail_after * 320
    assert unknown.generated_bytes == len(tts.audio)
    await transport.incoming.put(None)
    await task


@pytest.mark.asyncio
async def test_cancel_after_terminal_fact_attempt_never_sends_conflicting_cleanup_fact():
    transport, stt, tts, port = (
        FakeTransport(),
        FakeSTT(),
        FakeTTS(pcm(1000, 40)),
        FakeConversationPort(None),
    )
    terminal_entered = asyncio.Event()
    original = port.playback_fact

    async def lost_terminal_ack(event):
        await original(event)
        if event.state == PlaybackState.COMPLETED:
            terminal_entered.set()
            await asyncio.Event().wait()

    port.playback_fact = lost_terminal_ack
    call = MediaCall(transport, stt, tts, port)
    running = asyncio.create_task(call.run())
    await asyncio.wait_for(stt.started.wait(), 1)
    speaking = asyncio.create_task(call.speak("terminal-race", "Запрос обработан."))
    await asyncio.wait_for(terminal_entered.wait(), 2)
    speaking.cancel()
    with pytest.raises(asyncio.CancelledError):
        await speaking
    terminal = [
        fact
        for fact in port.playback
        if fact.state not in {PlaybackState.STARTED, PlaybackState.PARTIAL}
    ]
    assert len(terminal) == 1 and terminal[0].state == PlaybackState.COMPLETED
    await transport.incoming.put(None)
    await running


@pytest.mark.asyncio
async def test_call_cleanup_bounds_cancellation_resistant_resource_close():
    transport, stt, tts = FakeTransport(), FakeSTT(), FakeTTS()
    started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    events = []

    async def close_stubbornly():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()
        finished.set()

    tts.close = close_stubbornly
    call = MediaCall(
        transport,
        stt,
        tts,
        FakeConversationPort(response=None),
        settings=MediaSettings(cleanup_timeout=0.02),
        emit=lambda event, **values: events.append((event, values)),
    )
    task = asyncio.create_task(call.run())
    await stt.started.wait()
    await transport.incoming.put(None)
    await asyncio.wait_for(task, timeout=0.2)
    assert call.state.value == "closed" and started.is_set()
    assert not finished.is_set()
    assert ("media_cleanup_timeout", {"resource": "tts"}) in events
    release.set()
    await asyncio.wait_for(finished.wait(), timeout=1)
