"""Runtime owns partially constructed calls until MediaCall.run takes ownership."""

import asyncio

import pytest

import voice_gateway.application as application
from tests.media.media_fakes import FakeSTT, FakeTTS, FakeWriter
from voice_gateway.settings import Settings


@pytest.mark.parametrize("stage", ["stt", "tts", "call"])
async def test_factory_failure_closes_every_created_resource(stage, monkeypatch):
    stt, tts, writer = FakeSTT(), FakeTTS(), FakeWriter()

    def fail(*args, **kwargs):
        raise ValueError("factory failure")

    runtime = application.VoiceGatewayRuntime(
        Settings(_env_file=None),
        stt_factory=fail if stage == "stt" else lambda: stt,
        tts_factory=fail if stage == "tts" else lambda: tts,
    )
    if stage == "call":
        monkeypatch.setattr(application, "MediaCall", fail)
    await runtime._handle_connection(asyncio.StreamReader(), writer)
    assert writer.closed and not runtime._sessions
    assert stage == "stt" or stt.stopped
    assert stage != "call" or tts.closed


async def test_factory_cleanup_continues_after_resource_self_cancellation(monkeypatch):
    stt, tts, writer = FakeSTT(), FakeTTS(), FakeWriter()

    async def cancel():
        raise asyncio.CancelledError("resource cancelled")

    def fail(*args, **kwargs):
        raise ValueError("factory failure")

    stt.stop_stream = cancel
    runtime = application.VoiceGatewayRuntime(
        Settings(_env_file=None), stt_factory=lambda: stt, tts_factory=lambda: tts
    )
    monkeypatch.setattr(application, "MediaCall", fail)
    with pytest.raises(asyncio.CancelledError):
        await runtime._handle_connection(asyncio.StreamReader(), writer)
    assert writer.closed and tts.closed and not runtime._sessions


async def test_rejected_connection_close_has_a_deadline():
    writer = FakeWriter()
    runtime = application.VoiceGatewayRuntime(
        Settings(_env_file=None, shutdown_timeout_seconds=0.03)
    )
    runtime._closing = True

    async def blocked():
        await asyncio.Future()

    writer.wait_closed = blocked
    await asyncio.wait_for(runtime._handle_connection(asyncio.StreamReader(), writer), 0.3)
    assert writer.closed and not runtime._sessions


async def test_runtime_shutdown_does_not_serialize_stubborn_connection_closes():
    release = asyncio.Event()
    runtime = application.VoiceGatewayRuntime(
        Settings(_env_file=None, shutdown_timeout_seconds=0.02)
    )
    closes = []

    async def session():
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                pass

    class StubbornTransport:
        async def close(self):
            closes.append(asyncio.current_task())
            await session()

    tasks = [asyncio.create_task(session()) for _ in range(8)]
    runtime._sessions = {task: StubbornTransport() for task in tasks}
    await asyncio.sleep(0)
    try:
        await asyncio.wait_for(runtime.stop(), 1.3)
        assert len(closes) == 8
    finally:
        release.set()
        await asyncio.gather(*tasks, *closes)
