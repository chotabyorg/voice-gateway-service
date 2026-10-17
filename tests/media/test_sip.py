import asyncio
import weakref
from types import SimpleNamespace as NS

import pytest

from tests.media.sip_native import native
from voice_gateway.sip import PCMBuffer, SIPClient, SIPSettings, called_did
from voice_gateway.routing import RoutingUnavailable

NUMBER = "+375170000000"


def settings(**values):
    return SIPSettings(
        **{
            "server": "192.0.2.1",
            "domain": "pbx.invalid",
            "username": "1234",
            "password": "test-only",
            "bind_address": "192.0.2.2",
            **values,
        }
    )


def received(uri="sip:+375170000000@pbx.invalid", headers="", peer="192.0.2.1:5060"):
    return NS(
        srcAddress=peer,
        wholeMsg=f"INVITE {uri} SIP/2.0\r\nFrom: <sip:+375170000099@pbx.invalid>\r\n{headers}\r\n",
    )


class Registry:
    def __init__(self):
        self.calls = {}

    def register(self, registration):
        if registration.number != NUMBER:
            raise RoutingUnavailable()
        self.calls[str(registration.call_id)] = registration.number


async def until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.005)


def test_dialed_did_is_never_used_as_the_caller_number():
    from voice_gateway.sip import caller_ani, caller_number

    # A trunk that presents the called DID in From must not poison the ANI.
    trunk = NS(
        srcAddress="192.0.2.1:5060",
        wholeMsg=(
            "INVITE sip:+375170000000@pbx.invalid SIP/2.0\r\n"
            "From: <sip:+375170000000@pbx.invalid>\r\n\r\n"
        ),
    )
    assert caller_number(trunk) == "+375170000000"
    assert caller_ani(trunk, "+375170000000") == ""
    assert caller_ani(received(), "+375170000000") == "+375170000099"


class QueuedTransport:
    """Minimal held call used to exercise the busy queue without native SIP."""

    def __init__(self):
        self.drained = 0
        self.prepared = 0

    async def drain_audio(self):
        self.drained += 1
        return True

    async def prepare(self):
        self.prepared += 1

    async def close(self):
        pass


async def test_busy_call_is_announced_then_admitted_when_a_slot_frees():
    from voice_gateway.sip import BUSY_MESSAGE

    announcements, admitted = [], []
    free = {"slot": False}

    async def handle(transport):
        admitted.append(transport)

    async def announce(transport, text):
        announcements.append(text)

    client = SIPClient(
        settings(max_calls=1),
        handle,
        Registry(),
        None,
        capacity=lambda: free["slot"],
        announce=announce,
    )
    transport = QueuedTransport()
    task = asyncio.create_task(client._serve_queued(transport))
    await until(lambda: announcements)
    assert admitted == []
    free["slot"] = True
    await asyncio.wait_for(task, 2)
    assert announcements == [BUSY_MESSAGE]
    assert admitted == [transport]
    assert transport.drained >= 1


async def test_busy_call_times_out_with_a_closing_notice():
    from voice_gateway.sip import BUSY_MESSAGE, BUSY_TIMEOUT_MESSAGE

    announcements, admitted = [], []

    async def handle(transport):
        admitted.append(transport)

    async def announce(transport, text):
        announcements.append(text)

    client = SIPClient(
        settings(max_calls=1, busy_hold_seconds=0.3),
        handle,
        Registry(),
        None,
        capacity=lambda: False,
        announce=announce,
    )
    await asyncio.wait_for(client._serve_queued(QueuedTransport()), 2)
    assert admitted == []
    assert announcements == [BUSY_MESSAGE, BUSY_TIMEOUT_MESSAGE]


def test_did_is_called_number_never_caller_or_static_pilot():
    assert called_did(settings(), received()) == NUMBER
    assert called_did(settings(), received(uri="sip:375170000000@pbx.invalid")) == NUMBER
    for data in (
        received(uri="sip:1234@pbx.invalid"),
        received(peer="192.0.2.3:5060"),
        received(headers=" X-Called-Number: sip:+375170000000@pbx.invalid\r\n"),
    ):
        with pytest.raises(RoutingUnavailable):
            called_did(settings(), data)
    config = settings(called_party_header="P-Called-Party-ID")
    value = "P-Called-Party-ID: <sip:+375170000000@pbx.invalid>\r\n"
    assert called_did(config, received(uri="sip:1234@pbx.invalid", headers=value)) == NUMBER
    with pytest.raises(RoutingUnavailable):
        called_did(config, received(headers=value + value))
    with pytest.raises(RoutingUnavailable):
        called_did(config, received())


async def test_native_registration_media_confirmation_and_cleanup():
    pj, registry, started = native(), Registry(), []

    async def handle(transport):
        await transport.prepare()
        started.append(transport)
        while await transport.read_audio_chunk() is not None:
            pass

    client = SIPClient(settings(), handle, registry, pj)
    await client.start()
    endpoint = client.endpoint
    try:
        assert endpoint.conf.uaConfig.threadCnt == 0
        assert not endpoint.conf.logConfig.msgLogging
        assert endpoint.codecs == [("PCMA/8000/1", 255), ("PCMU/8000/1", 254), ("opus/48000/2", 0)]
        client._incoming(42, received())
        call = pj.calls[-1]
        assert call.answer_code == 200 and registry.calls[call.transport.call_id] == NUMBER
        call.activate_media()
        await asyncio.sleep(0.02)
        assert not started
        call.confirm()
        await until(lambda: bool(started))
        call.confirm()  # Duplicate callbacks cannot start another media owner.
        assert len(started) == 1
        assert call.transport.media_port in call.media_native.targets
        call.disconnect()
        await until(lambda: not client.tasks and not client.calls)
        assert started[0].audio.closed
    finally:
        await client.stop()
    assert endpoint.destroyed


async def test_unknown_did_peer_and_extension_never_start_a_session():
    pj, registry = native(), Registry()

    async def forbidden(transport):
        pytest.fail("Rejected SIP call reached media")

    client = SIPClient(settings(), forbidden, registry, pj)
    await client.start()
    try:
        for request in (
            received(uri="sip:1234@pbx.invalid"),
            received(peer="192.0.2.9:5060"),
            received(uri="sip:+375170000001@pbx.invalid"),
        ):
            client._incoming(42, request)
            call = pj.calls[-1]
            assert call.answer_code == 403
            call.confirm()
            call.activate_media()
            call.disconnect()
            await until(lambda: not client.calls)
        assert not registry.calls and not client.tasks
    finally:
        await client.stop()


async def test_capacity_transfer_rejection_and_per_call_buffers():
    pj, registry = native(), Registry()

    async def handle(transport):
        while await transport.read_audio_chunk() is not None:
            pass

    client = SIPClient(settings(max_calls=1), handle, registry, pj)
    await client.start()
    try:
        client._incoming(42, received())
        call = pj.calls[-1]
        client._incoming(43, received())
        rejected = pj.calls[-1]
        assert rejected.answer_code == 486
        prm = NS(statusCode=200)
        call.onCallTransferRequest(prm)
        assert prm.statusCode == 603
        prm.statusCode = 200
        call.onCallReplaceRequest(prm)
        assert prm.statusCode == 603
        assert not hasattr(client, "originate")
        await call.transport.write_audio_chunk(b"\x01\x00" * 160)
        assert call.transport.audio.request(320) == b"\x01\x00" * 160
        await call.transport.write_audio_chunk(b"\x01\x00" * 160)
        await call.transport.clear_output()
        assert call.transport.audio.request(320) == b"\0" * 320
    finally:
        await client.stop()


async def test_registration_failure_releases_native_endpoint():
    pj = native()
    pj.registration_code = 403
    client = SIPClient(settings(), None, Registry(), pj)
    with pytest.raises(ConnectionError):
        await client.start()
    assert pj.endpoints[0].destroyed and not client.registered


async def test_media_entries_keep_the_native_call_info_owner_alive():
    pj, registry = native(), Registry()
    client = SIPClient(settings(), None, registry, pj)
    await client.start()
    try:
        client._incoming(42, received())
        call = pj.calls[-1]

        class BorrowedMedia:
            def __init__(self, owner):
                self.owner = weakref.ref(owner)

            def __getattr__(self, key):
                assert self.owner() is not None, "Native media outlived CallInfo"
                return {
                    "type": pj.PJMEDIA_TYPE_AUDIO,
                    "status": pj.PJSUA_CALL_MEDIA_ACTIVE,
                    "index": 0,
                }[key]

        class CallInfo:
            def __init__(self):
                self.media = [BorrowedMedia(self)]

        call.getInfo = CallInfo
        call.onCallMediaState(NS())
        assert call.transport.ready.is_set() and call.media is not None
        assert not call.hangups
    finally:
        await client.stop()


async def test_account_shutdown_error_still_releases_endpoint():
    pj = native()
    client = SIPClient(settings(), None, Registry(), pj)
    await client.start()

    def fail():
        raise RuntimeError("Synthetic account shutdown error")

    client.account.shutdown = fail
    with pytest.raises(RuntimeError, match="Synthetic"):
        await client.stop()
    assert pj.endpoints[0].destroyed and not client.registered and client.account is None


def test_pcm_overflow_and_bad_native_frames_fail_instead_of_dropping_speech():
    buffer = PCMBuffer()
    for _ in range(50):
        buffer.receive(b"\0" * 320)
    buffer.receive(b"\0" * 320)
    with pytest.raises(ConnectionError):
        buffer.read()
    assert buffer.closed
    for frame in (b"x", b"\0" * 4098):
        buffer = PCMBuffer()
        buffer.receive(frame)
        with pytest.raises(ConnectionError):
            buffer.read()
    buffer = PCMBuffer()
    assert buffer.request(10**9) == b""
    with pytest.raises(ConnectionError):
        buffer.read()
