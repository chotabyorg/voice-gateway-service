"""Media/HTTP separation, permission correlation and failure fencing."""

import asyncio
import json
import time
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr

from tests.media.media_fakes import FakeSTT, FakeTTS, FakeTransport, pcm
from voice_gateway.conversation_http import (
    ConversationBoundaryError,
    ConversationHTTPPort,
    TrustedVoiceBinding,
)
from voice_gateway.events import CallEndReason, CallEnded, CallStarted, FinalUtterance
from voice_gateway.media.call import CallState, MediaCall, MediaSettings


class Server:
    def __init__(self):
        self.started = asyncio.Event()
        self.finalized = asyncio.Event()
        self.completed = asyncio.Event()
        self.facts = []
        self.inputs = []
        self.turn = str(uuid4())
        self.response = str(uuid4())
        self.available = False
        self.begun = False
        self.fail_heartbeat = False
        self.wrong_identity = False
        self.lost_permission = False
        self.ended = False

    async def handle(self, request):
        data = json.loads(request.content)
        identity = {k: data[k] for k in ("call_id", "media_session_id")}
        if self.wrong_identity:
            identity["call_id"] = str(uuid4())
        path = request.url.path.rsplit("/voice/", 1)[1]
        if path in {"calls/start", "calls/heartbeat", "calls/end"}:
            self.started.set()
            if path == "calls/heartbeat" and self.fail_heartbeat:
                return httpx.Response(503)
            self.ended |= path == "calls/end"
            return httpx.Response(
                200,
                json=dict(
                    identity,
                    conversation_id=str(uuid4()),
                    state="ENDED" if self.ended else "ACTIVE",
                    replayed=False,
                ),
            )
        if path == "utterances":
            self.inputs.append(data)
            self.finalized.set()
            return httpx.Response(
                200,
                json=dict(
                    identity, sequence=data["sequence"], message_id=str(uuid4()), replayed=False
                ),
            )
        if path == "responses/next":
            return httpx.Response(
                200,
                json=dict(
                    identity, turn_id=self.turn if self.available and not self.begun else None
                ),
            )
        if path == "responses/begin":
            self.begun = True
            if self.lost_permission:
                raise httpx.ReadTimeout("fixture")
            return httpx.Response(
                200,
                json=dict(
                    identity,
                    turn_id=self.turn,
                    response_id=self.response,
                    send=True,
                    text="Ответ",
                    replayed=False,
                ),
            )
        if path == "playback":
            self.facts.append(data)
            if data["terminal"]:
                self.completed.set()
            return httpx.Response(
                200,
                json=dict(
                    identity,
                    response_id=data["response_id"],
                    event_sequence=data["event_sequence"],
                    replayed=False,
                ),
            )
        raise AssertionError(path)


async def resolve(call_id):
    return TrustedVoiceBinding("fixture-owner", uuid4(), uuid4())


def adapter(server):
    return ConversationHTTPPort(
        "http://conversation.test",
        SecretStr("a" * 32),
        resolve,
        transport=httpx.MockTransport(server.handle),
        heartbeat_seconds=0.25,
        poll_seconds=0.05,
    )


async def test_media_accepts_new_final_during_playback_and_reports_completion():
    server = Server()
    port = adapter(server)
    transport, stt, tts = FakeTransport(), FakeSTT(), FakeTTS(pcm(1500, 600))
    transport.call_id = str(uuid4())
    call = MediaCall(
        transport,
        stt,
        tts,
        port,
        settings=MediaSettings(turn_silence_seconds=0, continuation_seconds=0),
    )
    task = asyncio.create_task(call.run())
    try:
        await asyncio.wait_for(server.started.wait(), 2)
        await stt.emit_final("Первый вопрос")
        await asyncio.wait_for(server.finalized.wait(), 2)
        server.available = True
        await asyncio.wait_for(transport.wrote.wait(), 2)
        server.finalized.clear()
        await stt.emit_final("Уточнение пока звучит ответ")
        await asyncio.wait_for(server.finalized.wait(), 0.3)
        assert len(server.inputs) == 2 and not server.completed.is_set()
        await asyncio.wait_for(server.completed.wait(), 2)
        assert server.facts[-1]["state"] == "COMPLETED"
        assert server.facts[-1]["generated_bytes"] == len(tts.audio)
        await transport.incoming.put(None)
        await asyncio.wait_for(task, 2)
        assert server.ended and not port.sessions
        assert stt.stopped and tts.closed and transport.closed
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await port.close()


async def test_heartbeat_failure_stops_active_audio_and_cleans_resources(caplog):
    server = Server()
    server.available = True
    port = adapter(server)
    transport, stt, tts = FakeTransport(), FakeSTT(), FakeTTS(pcm(1500, 5000))
    transport.call_id = str(uuid4())
    call = MediaCall(transport, stt, tts, port)
    task = asyncio.create_task(call.run())
    try:
        await asyncio.wait_for(transport.wrote.wait(), 2)
        server.fail_heartbeat = True
        with pytest.raises(ConversationBoundaryError):
            await asyncio.wait_for(task, 2)
        written = len(transport.sent)
        await asyncio.sleep(0.1)
        assert written == len(transport.sent)
        assert call.state == CallState.CLOSED and server.ended
        assert stt.stopped and tts.closed and not port.sessions
        diagnostic = [
            record.getMessage()
            for record in caplog.records
            if record.getMessage().startswith("conversation_boundary_failure")
        ]
        assert any(
            "operation=calls/heartbeat stage=HTTP_STATUS http_status=503" in line
            for line in diagnostic
        )
        assert not any("Ответ" in line or "a" * 32 in line for line in diagnostic)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await port.close()


async def test_lost_permission_is_not_retried_or_spoken():
    server = Server()
    server.available = server.lost_permission = True
    port = adapter(server)
    call_id = str(uuid4())
    try:
        await port.call_started(CallStarted(call_id, time.monotonic_ns()))
        with pytest.raises(ConversationBoundaryError):
            await anext(port.responses(call_id))
        assert server.begun and not server.facts
        await port.call_ended(CallEnded(call_id, CallEndReason.FAILED, time.monotonic_ns()))
    finally:
        await port.close()


async def test_cross_call_response_is_rejected_and_call_start_is_cleaned_up():
    server = Server()
    server.wrong_identity = True
    port = adapter(server)
    try:
        with pytest.raises(ConversationBoundaryError):
            await port.call_started(CallStarted(str(uuid4()), time.monotonic_ns()))
        assert not port.sessions
    finally:
        await port.close()


async def test_admission_returns_without_waiting_for_ai():
    server = Server()
    port = adapter(server)
    call_id = str(uuid4())
    try:
        await port.call_started(CallStarted(call_id, time.monotonic_ns()))
        assert await port.final_utterance(FinalUtterance(call_id, 1, "Привет", 0)) is None
        assert server.inputs and not server.begun
        await port.call_ended(CallEnded(call_id, CallEndReason.DISCONNECTED, 0))
    finally:
        await port.close()


async def test_local_speech_group_correlates_existing_wire_turn_without_changing_facts():
    server = Server()
    port = adapter(server)
    call_id = str(uuid4())
    responses = port.responses(call_id)
    try:
        await port.call_started(CallStarted(call_id, time.monotonic_ns()))
        server.available = True
        speech = await anext(responses)
        assert speech.request_id == server.response
        assert speech.response_group == server.turn
        assert speech.is_partial is False
    finally:
        await responses.aclose()
        await port.close()


async def test_blank_authorized_text_reports_not_started_instead_of_hanging_output():
    from tests.media.media_fakes import FakeConversationPort

    evidence = FakeConversationPort(None)
    call = MediaCall(FakeTransport(), FakeSTT(), FakeTTS(), evidence)
    call.state = CallState.ACTIVE
    assert await call.speak("blank", "  \n ") is False
    assert evidence.playback[-1].state.value == "not_started"
    assert evidence.playback[-1].transport_accepted_bytes == 0
    assert not call.tts.texts


@pytest.mark.parametrize("kind", ["oversized", "encoding", "duplicate_json"])
async def test_untrusted_response_shape_is_rejected(kind):
    async def invalid(request):
        data = json.loads(request.content)
        identity = {k: data[k] for k in ("call_id", "media_session_id")}
        body = dict(identity, conversation_id=str(uuid4()), state="ACTIVE", replayed=False)
        if kind == "oversized":
            return httpx.Response(
                200, content=b" " * 65537, headers={"content-type": "application/json"}
            )
        if kind == "encoding":
            return httpx.Response(200, json=body, headers={"content-encoding": "unsupported"})
        serialized = json.dumps(body)[:-1] + ',"state":"ACTIVE"}'
        return httpx.Response(200, content=serialized, headers={"content-type": "application/json"})

    port = ConversationHTTPPort(
        "http://conversation.test",
        SecretStr("a" * 32),
        resolve,
        transport=httpx.MockTransport(invalid),
    )
    try:
        with pytest.raises(ConversationBoundaryError):
            await port.call_started(CallStarted(str(uuid4()), 0))
        assert not port.sessions
    finally:
        await port.close()
