"""Slow local consumers must not create an unbounded acoustic transcript buffer."""

import asyncio

import pytest

from voice_gateway.media.utterances import UtteranceCoordinator
from voice_gateway.media.batch_stt import BatchSTT, BatchSTTSettings
from voice_gateway.media.call import MediaCall, MediaSettings
from voice_gateway.media.vad import VADSettings
from tests.media.media_fakes import FakeTransport, FakeTTS, FakeConversationPort, pcm


@pytest.mark.parametrize("limit", ["count", "chars"])
async def test_backpressure_preserves_order_and_releases_waiter(limit):
    entered, release = asyncio.Event(), asyncio.Event()
    delivered, events = [], []

    async def consume(text):
        delivered.append(text)
        if len(delivered) == 1:
            entered.set()
            await release.wait()

    turns = UtteranceCoordinator(
        consume,
        silence_seconds=0,
        continuation_seconds=0,
        max_pending=1 if limit == "count" else 8,
        max_pending_chars=100 if limit == "count" else 7,
        emit=lambda event, **values: events.append(event),
    )
    await turns.submit("first")
    await entered.wait()
    await turns.submit("second")
    waiting = asyncio.create_task(turns.submit("third"))
    await asyncio.sleep(0)
    assert not waiting.done() and turns.pending == ["second"]
    assert turns.pending_chars == 6 and "media_utterance_backpressure" in events
    release.set()
    await asyncio.wait_for(waiting, 1)
    await turns.task
    assert delivered == ["first", "second", "third"]
    assert turns.pending_chars == 0
    await turns.close()


async def test_close_releases_blocked_submit_without_late_dispatch():
    entered = asyncio.Event()

    async def consume(text):
        entered.set()
        await asyncio.Future()

    turns = UtteranceCoordinator(consume, max_pending=1)
    await turns.submit("first")
    await entered.wait()
    await turns.submit("second")
    waiting = asyncio.create_task(turns.submit("third"))
    await asyncio.sleep(0)
    await turns.close()
    await asyncio.wait_for(waiting, 1)
    assert not turns.pending and turns.pending_chars == 0
    assert turns.task.done()


async def test_oversized_final_is_rejected_without_retaining_it():
    turns = UtteranceCoordinator(None, max_pending_chars=10)
    with pytest.raises(ValueError, match="character limit"):
        await turns.submit("x" * 11)
    assert turns.task is None and not turns.pending


async def test_slow_consumer_saturates_bounded_stt_queue_and_closes_call():
    entered = asyncio.Event()

    class STT(BatchSTT):
        async def _transcribe(self, audio):
            return "fixture"

    class SlowPort(FakeConversationPort):
        async def final_utterance(self, event):
            entered.set()
            await asyncio.Future()

    transport = FakeTransport()
    stt = STT(
        vad=VADSettings(silence_end_ms=20),
        queue=BatchSTTSettings(max_pending=3, shutdown_timeout=0.05),
    )
    call = MediaCall(
        transport,
        stt,
        FakeTTS(),
        SlowPort(),
        settings=MediaSettings(
            turn_silence_seconds=0,
            max_pending_finals=1,
            cleanup_timeout=0.2,
        ),
    )
    task = asyncio.create_task(call.run())
    await transport.prepared.wait()
    try:
        await transport.incoming.put(pcm(1200, 100) + pcm(0, 20))
        await asyncio.wait_for(entered.wait(), 1)
        for _ in range(12):
            await transport.incoming.put(pcm(1200, 100) + pcm(0, 20))
            await asyncio.sleep(0)
            assert len(stt._active_tasks) <= 3
            assert len(call.utterances.pending) <= 1
        with pytest.raises(RuntimeError, match="backlog"):
            await asyncio.wait_for(task, 1)
        assert transport.closed and not stt._active_tasks and call.utterances.closed
        assert call.tts.closed and call.state.value == "closed"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
