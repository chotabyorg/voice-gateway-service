"""Barge-in must not reuse a playback sequence whose POST already committed."""

import asyncio
import json
from uuid import uuid4

import httpx
import pytest

from tests.media.media_fakes import FakeSTT, FakeTTS, FakeTransport, pcm
from tests.media.test_conversation_http import Server, adapter
from voice_gateway.conversation_http import ConversationBoundaryError
from voice_gateway.events import CallStarted, PlaybackFact, PlaybackState
from voice_gateway.media.call import CallState, MediaCall, MediaSettings


class CommittedAckServer(Server):
    def __init__(self, blocked_state):
        super().__init__()
        self.blocked_state = blocked_state
        self.committed = asyncio.Event()
        self.release_ack = asyncio.Event()
        self.attempts = []
        self.cancelled_request = False

    async def handle(self, request):
        if request.url.path.endswith("/playback"):
            data = json.loads(request.content)
            self.attempts.append(data)
            if data["event_sequence"] != len(self.facts) + 1:
                return httpx.Response(409)
            # The server commits before the client receives the ACK.
            response = await super().handle(request)
            if data["state"] == self.blocked_state:
                self.committed.set()
                try:
                    await self.release_ack.wait()
                except asyncio.CancelledError:
                    self.cancelled_request = True
                    raise
            return response
        return await super().handle(request)


def fact(call_id, request_id, state, accepted=8320):
    return PlaybackFact(call_id, request_id, state, accepted, 32000, 0)


@pytest.mark.parametrize("state", [PlaybackState.STARTED, PlaybackState.PARTIAL])
async def test_cancelled_progress_settles_ack_before_terminal_uses_next_sequence(state):
    server = CommittedAckServer(state.value.upper())
    port = adapter(server)
    call_id = str(uuid4())
    responses = port.responses(call_id)
    report = None
    try:
        await port.call_started(CallStarted(call_id, 0))
        server.available = True
        speech = await anext(responses)
        report = asyncio.create_task(port.playback_fact(fact(call_id, speech.request_id, state)))
        await asyncio.wait_for(server.committed.wait(), 1)
        report.cancel()
        await asyncio.sleep(0)
        report.cancel()  # repeated shutdown/barge-in cancellation is also bounded
        server.release_ack.set()
        with pytest.raises(asyncio.CancelledError):
            await report
        await port.playback_fact(fact(call_id, speech.request_id, PlaybackState.INTERRUPTED))
        assert [item["event_sequence"] for item in server.facts] == [1, 2]
        assert [item["state"] for item in server.facts] == [state.value.upper(), "INTERRUPTED"]
        assert not server.cancelled_request
        assert not port.sessions[call_id].failed.is_set()
    finally:
        server.release_ack.set()
        if report is not None:
            await asyncio.gather(report, return_exceptions=True)
        await responses.aclose()
        await port.close()


async def test_cancelled_progress_with_lost_ack_fails_within_request_deadline():
    server = CommittedAckServer("PARTIAL")
    port = adapter(server)
    port.timeout = 0.05
    call_id = str(uuid4())
    responses = port.responses(call_id)
    try:
        await port.call_started(CallStarted(call_id, 0))
        server.available = True
        speech = await anext(responses)
        report = asyncio.create_task(
            port.playback_fact(fact(call_id, speech.request_id, PlaybackState.PARTIAL))
        )
        await asyncio.wait_for(server.committed.wait(), 1)
        report.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(report, 0.3)
        assert port.sessions[call_id].failed.is_set()
        with pytest.raises(ConversationBoundaryError):
            await port.playback_fact(fact(call_id, speech.request_id, PlaybackState.INTERRUPTED))
        assert len(server.attempts) == 1  # unknown ACK is not retried with new facts
    finally:
        server.release_ack.set()
        await responses.aclose()
        await port.close()


async def test_acoustic_barge_in_during_committed_partial_ack_keeps_call_active():
    server = CommittedAckServer("PARTIAL")
    server.available = True
    port = adapter(server)
    transport, stt, tts = FakeTransport(), FakeSTT(), FakeTTS(pcm(1500, 2000))
    transport.call_id = str(uuid4())
    call = MediaCall(transport, stt, tts, port, settings=MediaSettings(smooth_barge_in=False))
    task = asyncio.create_task(call.run())
    try:
        await asyncio.wait_for(server.committed.wait(), 2)
        producer = call._output_task
        for _ in range(6):
            await transport.incoming.put(pcm(3000))
        async with asyncio.timeout(1):
            while not producer.cancelling():
                await asyncio.sleep(0.001)
        server.release_ack.set()
        await asyncio.wait_for(server.completed.wait(), 1)
        await asyncio.sleep(0.05)
        assert call.state == CallState.ACTIVE and not task.done()
        assert [item["state"] for item in server.facts] == ["STARTED", "PARTIAL", "INTERRUPTED"]
        assert [item["event_sequence"] for item in server.facts] == [1, 2, 3]
        assert all(item["transport_accepted_bytes"] <= 8320 for item in server.facts)
        assert not server.cancelled_request
        await transport.incoming.put(None)
        await asyncio.wait_for(task, 2)
    finally:
        server.release_ack.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await port.close()
