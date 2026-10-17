"""Caller-number (ANI) extraction and opt-in forwarding to Conversation."""

import json
import time
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr

from voice_gateway.conversation_http import ConversationHTTPPort, TrustedVoiceBinding
from voice_gateway.events import CallEndReason, CallEnded, CallStarted
from voice_gateway.sip import caller_number


class _Received:
    def __init__(self, message: str):
        self.wholeMsg = message


def _invite(extra_lines):
    lines = [
        "INVITE sip:+375173820015@chota.pbx.bn.by SIP/2.0",
        *extra_lines,
        "",
        "",
    ]
    return _Received("\r\n".join(lines))


def test_caller_number_prefers_asserted_identity_over_from():
    received = _invite(
        [
            'From: "Client" <sip:+375291112233@operator.example>;tag=1',
            "P-Asserted-Identity: <sip:+375447778899@operator.example>",
            "X-Called-Number: +375173820015",
        ]
    )
    assert caller_number(received) == "+375447778899"


def test_caller_number_falls_back_to_from_and_rejects_non_belarusian():
    assert (
        caller_number(
            _invite(
                [
                    "From: <sip:+375291112233@gw>;tag=1",
                ]
            )
        )
        == "+375291112233"
    )
    assert (
        caller_number(
            _invite(
                [
                    "From: <sip:+15551234567@gw>;tag=1",
                ]
            )
        )
        == ""
    )
    assert caller_number(_invite(["From: anonymous;tag=1"])) == ""


def _binding(call_id):
    return TrustedVoiceBinding("owner", uuid4(), uuid4())


async def _resolve(call_id):
    return _binding(call_id)


def _port(seen, *, send: bool):
    async def handle(request):
        data = json.loads(request.content)
        identity = {k: data[k] for k in ("call_id", "media_session_id")}
        path = request.url.path.rsplit("/voice/", 1)[1]
        if path == "calls/start":
            seen.append(data)
            return httpx.Response(
                200,
                json=dict(identity, conversation_id=str(uuid4()), state="ACTIVE", replayed=False),
            )
        if path == "calls/end":
            return httpx.Response(
                200,
                json=dict(identity, conversation_id=str(uuid4()), state="ENDED", replayed=False),
            )
        raise AssertionError(path)

    return ConversationHTTPPort(
        "http://conversation.test",
        SecretStr("a" * 32),
        _resolve,
        transport=httpx.MockTransport(handle),
        send_caller_number=send,
    )


@pytest.mark.asyncio
async def test_calls_start_carries_only_a_validated_caller_number():
    seen = []
    port = _port(seen, send=True)
    call_id = str(uuid4())
    try:
        await port.call_started(CallStarted(call_id, time.monotonic_ns(), "+375291112233"))
        await port.call_started(CallStarted(str(uuid4()), 0, "+15551234567"))
        await port.call_started(CallStarted(str(uuid4()), 0, ""))
        await port.call_ended(CallEnded(call_id, CallEndReason.DISCONNECTED, 0))
    finally:
        await port.close()
    assert seen[0]["caller_number"] == "+375291112233"
    assert "caller_number" not in seen[1]
    assert "caller_number" not in seen[2]


@pytest.mark.asyncio
async def test_caller_number_is_not_sent_when_the_switch_is_off():
    seen = []
    port = _port(seen, send=False)
    call_id = str(uuid4())
    try:
        await port.call_started(CallStarted(call_id, 0, "+375291112233"))
        await port.call_ended(CallEnded(call_id, CallEndReason.DISCONNECTED, 0))
    finally:
        await port.close()
    assert "caller_number" not in seen[0]
