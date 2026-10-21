"""Replay only identical evidence after ambiguous HTTP transport failures."""

import asyncio
import json
from uuid import uuid4

import httpx
import pytest

from tests.media.test_conversation_http import Server, adapter
from tests.media.test_playback_ack_cancellation import fact
from voice_gateway.conversation_http import ConversationBoundaryError
from voice_gateway.events import CallStarted, FinalUtterance, PlaybackState


class ReplayServer(Server):
    def __init__(self, mode="committed"):
        super().__init__()
        self.mode = mode
        self.attempts = []
        self.retry_started = asyncio.Event()
        self.release_retry = asyncio.Event()

    async def handle(self, request):
        if not request.url.path.endswith("/playback"):
            return await super().handle(request)
        data = json.loads(request.content)
        self.attempts.append(request.content)
        if len(self.attempts) == 1:
            if self.mode in {"committed", "cancelled"}:
                await super().handle(request)
            raise httpx.ReadError("must not log customer text or secret")
        if self.mode == "always_fails":
            raise httpx.ReadTimeout("must not log customer text or secret")
        if self.mode == "deadline":
            # The second request stays pending until the original timeout.
            # Do not rely on two 30 ms sleeps fitting a 50 ms Windows budget.
            await asyncio.Event().wait()
        if self.mode == "cancelled":
            self.retry_started.set()
            await self.release_retry.wait()
        previous = next(
            (item for item in self.facts if item["event_sequence"] == data["event_sequence"]),
            None,
        )
        if previous is not None:
            assert previous == data  # Conversation rejects a conflicting fingerprint.
            return httpx.Response(
                200,
                json={
                    **{
                        key: data[key]
                        for key in ("call_id", "media_session_id", "response_id", "event_sequence")
                    },
                    "replayed": True,
                },
            )
        return await super().handle(request)


@pytest.mark.parametrize("mode", ["committed", "before_commit"])
@pytest.mark.parametrize("state", [PlaybackState.PARTIAL, PlaybackState.COMPLETED])
async def test_transport_retry_keeps_exact_bytes_and_advances_sequence_once(mode, state, caplog):
    server = ReplayServer(mode)
    port = adapter(server)
    call_id = str(uuid4())
    responses = port.responses(call_id)
    try:
        await port.call_started(CallStarted(call_id, 0))
        server.available = True
        speech = await anext(responses)
        await port.playback_fact(fact(call_id, speech.request_id, state))
        assert len(server.attempts) == 2 and server.attempts[0] == server.attempts[1]
        assert len(server.facts) == 1
        assert port.sessions[call_id].event_sequence == 1
        assert not port.sessions[call_id].failed.is_set()
        assert "conversation_playback_transport_retry kind=NETWORK" in caplog.text
        assert "must not log" not in caplog.text
    finally:
        await responses.aclose()
        await port.close()


@pytest.mark.parametrize("mode", ["always_fails", "deadline"])
async def test_second_failure_or_original_deadline_fences_later_facts(mode, caplog, monkeypatch):
    server = ReplayServer(mode)
    port = adapter(server)
    port.timeout = 0.2
    call_id = str(uuid4())
    responses = port.responses(call_id)
    try:
        await port.call_started(CallStarted(call_id, 0))
        server.available = True
        speech = await anext(responses)
        timeout_scopes = []
        timeout = asyncio.timeout

        def record_timeout(delay):
            timeout_scopes.append(delay)
            return timeout(delay)

        monkeypatch.setattr(asyncio, "timeout", record_timeout)
        with pytest.raises(ConversationBoundaryError):
            await port.playback_fact(fact(call_id, speech.request_id, PlaybackState.PARTIAL))
        assert timeout_scopes == [port.timeout]  # One deadline owns both attempts.
        assert port.sessions[call_id].failed.is_set()
        assert port.sessions[call_id].event_sequence == 0
        assert len(server.attempts) == 2 and server.attempts[0] == server.attempts[1]
        assert not server.facts
        with pytest.raises(ConversationBoundaryError):
            await port.playback_fact(fact(call_id, speech.request_id, PlaybackState.INTERRUPTED))
        assert len(server.attempts) == 2
        expected = "READ_TIMEOUT" if mode == "always_fails" else "DEADLINE"
        assert f"conversation_boundary_cause operation=playback kind={expected}" in caplog.text
        assert "must not log" not in caplog.text
    finally:
        await responses.aclose()
        await port.close()


async def test_barge_in_during_retry_settles_replayed_sequence_before_interruption():
    server = ReplayServer("cancelled")
    port = adapter(server)
    call_id = str(uuid4())
    responses = port.responses(call_id)
    report = None
    try:
        await port.call_started(CallStarted(call_id, 0))
        server.available = True
        speech = await anext(responses)
        report = asyncio.create_task(
            port.playback_fact(fact(call_id, speech.request_id, PlaybackState.PARTIAL))
        )
        await asyncio.wait_for(server.retry_started.wait(), 1)
        report.cancel()
        await asyncio.sleep(0)
        report.cancel()
        server.release_retry.set()
        with pytest.raises(asyncio.CancelledError):
            await report
        await port.playback_fact(fact(call_id, speech.request_id, PlaybackState.INTERRUPTED))
        assert [item["event_sequence"] for item in server.facts] == [1, 2]
        assert [item["state"] for item in server.facts] == ["PARTIAL", "INTERRUPTED"]
        assert not port.sessions[call_id].failed.is_set()
    finally:
        server.release_retry.set()
        if report is not None:
            await asyncio.gather(report, return_exceptions=True)
        await responses.aclose()
        await port.close()


@pytest.mark.parametrize("mode", ["status", "headers", "json", "identity", "sequence"])
async def test_invalid_playback_reply_is_never_retried(mode):
    server = Server()
    port = adapter(server)
    call_id = str(uuid4())
    responses = port.responses(call_id)
    attempts = []
    original = server.handle

    async def handle(request):
        if not request.url.path.endswith("/playback"):
            return await original(request)
        attempts.append(request.content)
        if mode == "status":
            return httpx.Response(503)
        if mode == "headers":
            return httpx.Response(200, text="{}")
        if mode == "json":
            return httpx.Response(200, content=b"{", headers={"content-type": "application/json"})
        response = await original(request)
        body = response.json()
        body["call_id" if mode == "identity" else "event_sequence"] = (
            str(uuid4()) if mode == "identity" else 2
        )
        return httpx.Response(200, json=body)

    port.client._transport = httpx.MockTransport(handle)
    try:
        await port.call_started(CallStarted(call_id, 0))
        server.available = True
        speech = await anext(responses)
        with pytest.raises(ConversationBoundaryError):
            await port.playback_fact(fact(call_id, speech.request_id, PlaybackState.COMPLETED))
        assert len(attempts) == 1 and port.sessions[call_id].failed.is_set()
    finally:
        await responses.aclose()
        await port.close()


async def test_transport_error_on_utterance_is_not_retried():
    server = Server()
    port = adapter(server)
    attempts = []
    original = server.handle

    async def handle(request):
        if request.url.path.endswith("/utterances"):
            attempts.append(request.content)
            raise httpx.ReadError("fixture")
        return await original(request)

    port.client._transport = httpx.MockTransport(handle)
    call_id = str(uuid4())
    try:
        await port.call_started(CallStarted(call_id, 0))
        with pytest.raises(ConversationBoundaryError):
            await port.final_utterance(FinalUtterance(call_id, 1, "fixture", 0))
        assert len(attempts) == 1 and port.sessions[call_id].failed.is_set()
    finally:
        await port.close()
