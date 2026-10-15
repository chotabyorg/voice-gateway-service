"""A disconnected call must not wait for an unusable provider transcript."""

import asyncio

import pytest

from tests.media.media_fakes import FakeConversationPort, FakeTTS, pcm
from tests.media.sip_native import native
from tests.media.test_sip import Registry, received, settings, until
from voice_gateway.application import VoiceGatewayRuntime
from voice_gateway.media.batch_stt import BatchSTT, BatchSTTSettings
from voice_gateway.settings import Settings
from voice_gateway.sip import SIPClient


async def test_single_slot_accepts_redial_after_hangup_with_stt_in_flight():
    started, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    instances = []

    class SlowSTT(BatchSTT):
        async def _transcribe(self, audio):
            started.set()
            try:
                await release.wait()
                return "late synthetic transcript"
            finally:
                cancelled.set()

    def make_stt():
        stt = SlowSTT()
        instances.append(stt)
        return stt

    port, pj = FakeConversationPort(response=None), native()
    runtime = VoiceGatewayRuntime(
        Settings(_env_file=None, max_calls=1),
        conversation_port=port,
        stt_factory=make_stt,
        tts_factory=FakeTTS,
    )
    client = SIPClient(settings(max_calls=1), runtime.handle_transport, Registry(), pj)
    await client.start()
    try:
        client._incoming(42, received())
        first = pj.calls[-1]
        first.confirm()
        first.activate_media()
        await until(lambda: len(port.started) == 1)
        await instances[0].feed_audio(pcm(1000, 200))
        await instances[0]._flush_utterance()
        await asyncio.wait_for(started.wait(), 1)
        first.disconnect()
        async with asyncio.timeout(0.5):
            await until(lambda: not client.tasks and not client.calls and not runtime._sessions)
        assert cancelled.is_set() and not release.is_set()
        assert not port.utterances and len(port.ended) == 1
        # Native SIP call IDs can be reused immediately after disconnect.
        client._incoming(42, received())
        second = pj.calls[-1]
        assert second.answer_code == 200
        second.confirm()
        second.activate_media()
        await until(lambda: len(port.started) == 2)
        assert len(runtime._sessions) == 1
        second.disconnect()
        await until(lambda: not client.calls and not runtime._sessions)
    finally:
        release.set()
        await client.stop()


@pytest.mark.parametrize("resists_cancellation", [False, True])
async def test_abort_discards_buffered_tail_and_fences_late_provider_result(resists_cancellation):
    started, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    finals, calls, closed = [], [], []

    class STT(BatchSTT):
        async def _transcribe(self, audio):
            calls.append(audio)
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                if not resists_cancellation:
                    raise
                await release.wait()
            return "late synthetic transcript"

        async def _close(self):
            closed.append(True)

    async def on_final(text):
        finals.append(text)

    stt = STT(queue=BatchSTTSettings(concurrency=1))
    await stt.start_stream(on_final=on_final)
    try:
        await stt.feed_audio(pcm(1000, 200))
        await stt._flush_utterance()
        await asyncio.wait_for(started.wait(), 1)
        await stt.feed_audio(pcm(1000, 200))
        await stt._flush_utterance()  # Queued behind the in-flight provider call.
        await stt.feed_audio(pcm(1000, 200))  # Must never become another request.
        await asyncio.wait_for(stt.abort_stream(), 0.5)
        assert cancelled.is_set() and closed == [True]
        assert len(calls) == 1 and not finals and stt.vad.buffer_bytes == 0
        if resists_cancellation:
            assert stt._active_tasks  # Late result must be fenced after abort returns.
        with pytest.raises(RuntimeError, match="stopped"):
            await stt.feed_audio(pcm(1000))
    finally:
        release.set()
        if stt._active_tasks:
            await asyncio.gather(*tuple(stt._active_tasks), return_exceptions=True)
    assert not finals


async def test_abort_bounds_provider_close_even_without_pending_transcription():
    cancelled = asyncio.Event()

    class STT(BatchSTT):
        async def _close(self):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    stt = STT(queue=BatchSTTSettings(shutdown_timeout=0.04))
    await stt.start_stream()
    await stt.feed_audio(pcm(1000, 200))
    await asyncio.wait_for(stt.abort_stream(), 0.3)
    await asyncio.wait_for(cancelled.wait(), 0.3)
    assert stt.vad.buffer_bytes == 0 and not stt._active_tasks
