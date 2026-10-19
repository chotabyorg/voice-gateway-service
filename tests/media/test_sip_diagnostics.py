"""Rejected/short calls remain diagnosable without raw signaling or changed behavior."""

import asyncio
import json
import logging
from types import SimpleNamespace as NS

import pytest

from tests.media.sip_native import native
from tests.media.test_sip import Registry, received, settings, until
from voice_gateway import sip
from voice_gateway.logging_config import JsonFormatter


def events(caplog, event):
    return [r for r in caplog.records if getattr(r, "event", None) == event]


@pytest.mark.parametrize("code", [200, 403])
async def test_registration_result_is_visible_without_credentials(caplog, code):
    caplog.set_level(logging.INFO, logger=sip.__name__)
    pj = native()
    pj.registration_code = code
    client = sip.SIPClient(settings(), None, Registry(), pj)
    try:
        if code == 200:
            await client.start()
        else:
            with pytest.raises(ConnectionError):
                await client.start()
        (record,) = events(caplog, "sip_registration_state")
        assert record.sip_status == code and record.registered is (code == 200)
        serialized = JsonFormatter().format(record)
        assert "test-only" not in serialized and "192.0.2." not in serialized
    finally:
        await client.stop()


@pytest.mark.parametrize(
    "case,reason,status",
    [
        ("peer", "request_rejected", 403),
        ("extension", "request_rejected", 403),
        ("route", "route_registration_rejected", 403),
        ("unregistered", "registration_unavailable", 486),
        ("stopping", "stopping", 486),
        ("capacity", "capacity", 486),
    ],
)
async def test_inbound_rejection_retains_existing_status_and_closed_reason(
    caplog, case, reason, status
):
    caplog.set_level(logging.INFO, logger=sip.__name__)
    pj = native()
    client = sip.SIPClient(settings(max_calls=1), None, Registry(), pj)
    await client.start()
    try:
        request = received()
        if case == "peer":
            request = received(peer="192.0.2.9:5060")
        elif case == "extension":
            request = received(uri="sip:1234@pbx.invalid")
        elif case == "route":
            request = received(uri="sip:+375170000001@pbx.invalid")
        elif case == "unregistered":
            client.registered = False
        elif case == "stopping":
            client.stopping = True
        elif case == "capacity":
            client._incoming(41, request)
        caplog.clear()
        client._incoming(42, request)
        (record,) = events(caplog, "sip_inbound_decision")
        assert pj.calls[-1].answer_code == status == record.sip_status
        assert record.sip_reason == reason and not record.session_started
        assert pj.calls[-1].transport is None
        assert not client.tasks
        encoded = JsonFormatter().format(record)
        assert not any(
            secret in encoded for secret in ("375", "pbx.invalid", "INVITE", "test-only")
        )
    finally:
        await client.stop()


@pytest.mark.parametrize("local", [False, True])
async def test_disconnect_distinguishes_observed_local_request_without_claiming_remote_origin(
    caplog, local
):
    caplog.set_level(logging.INFO, logger=sip.__name__)
    pj = native()
    client = sip.SIPClient(settings(), None, Registry(), pj)
    await client.start()
    try:
        client._incoming(42, received())
        call = pj.calls[-1]
        original = call.getInfo

        def info():
            return NS(**vars(original()), lastStatusCode=200, lastReason="private SIP reason")

        call.getInfo = info
        if local:
            client.hangup(call)
        call.disconnect()
        call.disconnect()
        (record,) = events(caplog, "sip_call_disconnected")
        assert record.sip_status == 200 and record.local_hangup_requested is local
        assert not record.confirmed and not record.session_started
        assert call.transport.disconnected
        assert len(events(caplog, "sip_local_hangup_requested")) == int(local)
        payload = json.loads(JsonFormatter().format(record))
        assert payload["local_hangup_requested"] is local
        assert "private SIP reason" not in json.dumps(payload) and "remoteUri" not in payload
        await until(lambda: not client.calls)
    finally:
        await client.stop()


async def test_answer_failure_is_visible_and_still_releases_call(caplog, monkeypatch):
    caplog.set_level(logging.INFO, logger=sip.__name__)
    pj = native()
    client = sip.SIPClient(settings(), None, Registry(), pj)
    await client.start()
    try:

        def fail(call, options):
            raise RuntimeError("private failure detail")

        monkeypatch.setattr(client.call_type, "answer", fail)
        client._incoming(42, received())
        (record,) = events(caplog, "sip_answer_failed")
        assert record.sip_status == 200 and not record.session_started
        assert not client.calls and not client.tasks
        assert pj.calls[-1].finished and pj.calls[-1].transport.disconnected
        assert "private failure detail" not in JsonFormatter().format(record)
    finally:
        await client.stop()


async def test_established_session_has_confirmed_media_flags_at_disconnect(caplog):
    caplog.set_level(logging.INFO, logger=sip.__name__)
    pj = native()
    started = asyncio.Event()

    async def handle(transport):
        started.set()
        while await transport.read_audio_chunk() is not None:
            pass

    client = sip.SIPClient(settings(), handle, Registry(), pj)
    await client.start()
    try:
        client._incoming(42, received())
        call = pj.calls[-1]
        call.activate_media()
        call.confirm()
        await asyncio.wait_for(started.wait(), 1)
        call.disconnect()
        (record,) = events(caplog, "sip_call_disconnected")
        assert record.confirmed and record.media_ready and record.session_started
        assert not record.local_hangup_requested
        await until(lambda: not client.calls and not client.tasks)
    finally:
        await client.stop()


async def test_held_call_is_distinguished_from_rejected_capacity(caplog):
    caplog.set_level(logging.INFO, logger=sip.__name__)
    pj = native()
    client = sip.SIPClient(settings(), None, Registry(), pj, capacity=lambda: False)
    await client.start()
    try:
        client._incoming(42, received())
        (record,) = events(caplog, "sip_inbound_decision")
        assert record.sip_reason == "queued" and record.sip_status == 200
        assert pj.calls[-1].transport.queued and not record.session_started
    finally:
        await client.stop()


async def test_diagnostic_failure_cannot_change_admission_or_cleanup(monkeypatch):
    pj = native()
    client = sip.SIPClient(settings(), None, Registry(), pj)
    await client.start()
    try:
        with monkeypatch.context() as patch:

            def fail(*args, **kwargs):
                raise RuntimeError("log handler failed")

            patch.setattr(sip.logger, "info", fail)
            client._incoming(42, received())
            call = pj.calls[-1]
            assert call.answer_code == 200
            call.disconnect()
            assert call.finished and call.transport.disconnected
        await until(lambda: not client.calls)
    finally:
        await client.stop()


@pytest.mark.parametrize("status", [None, True, "200", 99, 700])
def test_untrusted_or_unavailable_native_status_is_not_fabricated(caplog, status):
    caplog.set_level(logging.INFO, logger=sip.__name__)
    client = sip.SIPClient(settings(), None, Registry(), native())
    client._diagnostic("sip_registration_state", status=status)
    (record,) = events(caplog, "sip_registration_state")
    assert "sip_status" not in json.loads(JsonFormatter().format(record))
