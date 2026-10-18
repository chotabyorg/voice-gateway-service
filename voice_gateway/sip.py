"""Inbound-only PJSUA2 host adapter, adapted from the direct-SIP donor.

SIP signaling runs on the event-loop thread; native media callbacks only move
bounded PCM. No outbound calling, donor session, history or AI dependencies.
"""

import asyncio
import gc
import ipaddress
import logging
import re
import threading
import time
from collections import deque
from uuid import uuid4

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from voice_gateway.media.audio import amplify_pcm, rms
from voice_gateway.media.completion import finish_owned_task
from voice_gateway.media.interfaces import CallTransport, TransportIdleTimeout
from voice_gateway.routing import Registration, RoutingUnavailable

logger = logging.getLogger(__name__)

# Spoken while every conversation session is busy; the caller is held on the
# line until a slot frees. Authorized agent speech still always comes from
# Conversation — these are fixed transport announcements only.
BUSY_MESSAGE = (
    "Сейчас достигнуто максимальное количество одновременных разговоров. "
    "Пожалуйста, ожидайте на линии."
)
BUSY_TIMEOUT_MESSAGE = "К сожалению, все линии всё ещё заняты. Пожалуйста, перезвоните чуть позже."


class SIPSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="VOICE_SIP_", extra="forbid", hide_input_in_errors=True
    )
    server: str
    domain: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    username: str = Field(pattern=r"^[A-Za-z0-9_.-]+$", repr=False)
    password: SecretStr
    bind_address: str
    port: int = Field(default=5060, ge=1, le=65535)
    server_port: int = Field(default=5060, ge=1, le=65535)
    rtp_port: int = Field(default=14000, ge=1024, le=65000)
    max_calls: int = Field(default=2, ge=1, le=32)
    max_waiting: int = Field(default=2, ge=0, le=8)
    busy_hold_seconds: float = Field(default=90, ge=0, le=600)
    registration_timeout: float = Field(default=15, gt=0, le=60)
    media_idle_timeout: float = Field(default=30, gt=0, le=300)
    input_gain: int = Field(default=1, ge=1, le=32)
    called_party_header: str = Field(default="", pattern=r"^(|P-Called-Party-ID|X-Called-Number)$")

    @model_validator(mode="after")
    def valid_network(self):
        ipaddress.IPv4Address(self.server)
        ipaddress.IPv4Address(self.bind_address)
        if (
            not self.password.get_secret_value()
            or self.rtp_port % 2
            or self.rtp_port + 2 * (self.max_calls + self.max_waiting) > 65535
        ):
            raise ValueError("Invalid SIP credentials or media port range")
        return self


def called_did(settings, received):
    """Accept only the configured PBX peer and its selected authoritative DID field.

    Header mode is opt-in: the PBX must overwrite that field, not forward a
    caller-supplied copy. An extension/unknown DID never becomes the pilot DID.
    """
    if received.srcAddress != f"{settings.server}:{settings.server_port}":
        raise RoutingUnavailable()
    raw = received.wholeMsg
    if not isinstance(raw, str) or len(raw) > 16384 or "\x00" in raw:
        raise RoutingUnavailable()
    headers = raw.split("\r\n\r\n", 1)[0].split("\r\n")
    first = re.fullmatch(r"INVITE (sips?:[^ ]+) SIP/2\.0", headers[0])
    if not first or any(line[:1] in {" ", "\t"} for line in headers[1:]):
        raise RoutingUnavailable()
    value = first[1]
    if settings.called_party_header:
        values = [
            line.partition(":")[2].strip()
            for line in headers[1:]
            if line.partition(":")[0].lower() == settings.called_party_header.lower()
        ]
        if len(values) != 1:
            raise RoutingUnavailable()
        value = values[0]
    match = re.fullmatch(
        r"<?sips?:(\+?375[0-9]{9})@[A-Za-z0-9_.:-]+(?:;[A-Za-z0-9=_.-]+)*>?", value
    )
    if match is None:
        raise RoutingUnavailable()
    return "+" + match[1].lstrip("+")


def caller_number(received) -> str:
    """Best-effort caller ANI from the validated INVITE; empty when absent.

    Never raises: a missing or non-belarusian caller id simply means the number
    is unknown, and the call still proceeds.
    """
    raw = getattr(received, "wholeMsg", "")
    if not isinstance(raw, str) or len(raw) > 16384 or "\x00" in raw:
        return ""
    headers = raw.split("\r\n\r\n", 1)[0].split("\r\n")
    preferred, fallback = "", ""
    for line in headers[1:]:
        name = line.partition(":")[0].strip().lower()
        if name not in {"from", "p-asserted-identity"}:
            continue
        value = line.partition(":")[2].strip()
        match = re.search(r"(?:\+?375)([0-9]{9})", value)
        if match is None:
            continue
        found = "+375" + match[1]
        if name == "p-asserted-identity":
            preferred = preferred or found
        else:
            fallback = fallback or found
    return preferred or fallback


def caller_ani(received, called_number: str) -> str:
    """Caller ANI from the validated INVITE, never the dialed DID.

    Some PBX trunks present the called DID in From/P-Asserted-Identity; a caller
    cannot ring their own number, so that value is not a caller identity.
    """
    ani = caller_number(received)
    return "" if ani == called_number else ani


class PCMBuffer:
    def __init__(self):
        self.lock = threading.Lock()
        self.incoming = deque()
        self.incoming_bytes = 0
        self.outgoing = bytearray()
        self.closed = False
        self.failed = False

    def receive(self, pcm):
        with self.lock:
            if self.closed:
                return
            if len(pcm) % 2 or len(pcm) > 4096 or self.incoming_bytes + len(pcm) > 16000:
                self.failed = True
                self.closed = True
                self.incoming.clear()
                self.outgoing.clear()
                return
            if pcm:
                self.incoming.append(pcm)
                self.incoming_bytes += len(pcm)

    def read(self):
        with self.lock:
            if self.failed:
                raise ConnectionError("SIP input buffer failed")
            if self.closed:
                return None
            pcm = self.incoming.popleft() if self.incoming else b""
            self.incoming_bytes -= len(pcm)
            return pcm

    def append(self, pcm):
        with self.lock:
            if self.closed:
                raise ConnectionError("SIP call closed")
            if len(self.outgoing) + len(pcm) > 3200:
                return False
            self.outgoing.extend(pcm)
            return True

    def request(self, size):
        with self.lock:
            if size < 0 or size > 4096 or size % 2:
                self.failed = self.closed = True
                self.outgoing.clear()
                return b""
            if self.closed:
                return b"\0" * size
            pcm = bytes(self.outgoing[:size])
            del self.outgoing[:size]
            return pcm.ljust(size, b"\0")

    def clear(self):
        with self.lock:
            self.outgoing.clear()

    def close(self):
        with self.lock:
            self.closed = True
            self.incoming.clear()
            self.outgoing.clear()
            self.incoming_bytes = 0


class SIPTransport(CallTransport):
    def __init__(self, client, call):
        self.client, self.call = client, call
        self.call_id = str(uuid4())
        self.caller_number = ""
        self.queued = False
        self.audio = PCMBuffer()
        self.ready = asyncio.Event()
        self.disconnected = False
        self.media_port = None
        self.input_bytes = self.input_rms_max = 0
        self.input_reported = False

    async def prepare(self):
        await asyncio.wait_for(self.ready.wait(), 10)
        if self.disconnected:
            raise ConnectionError("SIP media disconnected")

    async def read_audio_chunk(self):
        try:
            async with asyncio.timeout(self.client.settings.media_idle_timeout):
                while True:
                    pcm = self.audio.read()
                    if pcm is None:
                        return None
                    if pcm:
                        self.input_bytes += len(pcm)
                        self.input_rms_max = max(self.input_rms_max, rms(pcm))
                        return amplify_pcm(pcm, self.client.settings.input_gain)
                    await asyncio.sleep(0.01)
        except TimeoutError:
            raise TransportIdleTimeout("SIP media idle timeout") from None

    async def drain_audio(self):
        """Discard buffered caller audio while a held call waits for a slot.

        The bounded native input buffer fails the call if it is never read, so a
        queued caller's audio must be consumed while they wait on the line.
        """
        while True:
            pcm = self.audio.read()
            if pcm is None:
                return False
            if not pcm:
                return True
            self.input_bytes += len(pcm)
            self.input_rms_max = max(self.input_rms_max, rms(pcm))

    async def write_audio_chunk(self, chunk):
        if len(chunk) % 2 or len(chunk) > 65536:
            raise ValueError("Invalid SIP PCM frame")
        async with asyncio.timeout(3):
            for offset in range(0, len(chunk), 320):
                while not self.audio.append(chunk[offset : offset + 320]):
                    await asyncio.sleep(0.01)

    async def clear_output(self):
        self.audio.clear()

    async def close(self):
        self.audio.close()
        if not self.input_reported:
            self.input_reported = True
            logger.info(
                "sip_input_summary",
                extra={
                    "event": "sip_input_summary",
                    "audio_bytes": self.input_bytes,
                    "audio_rms_max": self.input_rms_max,
                    "input_gain": self.client.settings.input_gain,
                },
            )
        if not self.disconnected and self.call is not None:
            self.client.hangup(self.call)

    def remote_closed(self):
        self.disconnected = True
        self.audio.close()
        self.ready.set()


class SIPClient:
    def __init__(
        self, settings, handle_transport, registry, pj=None, *, capacity=None, announce=None
    ):
        self.settings, self.handle_transport, self.registry, self.pj = (
            settings,
            handle_transport,
            registry,
            pj,
        )
        self.capacity = capacity
        self.announce = announce
        self.endpoint = self.account = self.poll_task = self.close_task = None
        self.calls, self.tasks = {}, set()
        self.registration = asyncio.Event()
        self.registered = self.stopping = False

    async def start(self):
        if self.pj is None:
            import pjsua2

            self.pj = pjsua2
        pj = self.pj
        self.endpoint = pj.Endpoint()
        self.endpoint.libCreate()
        try:
            ep = pj.EpConfig()
            ep.uaConfig.threadCnt, ep.uaConfig.mainThreadOnly = 0, True
            required_capacity = self.settings.max_calls + self.settings.max_waiting
            # PJSUA2 2.17 initializes this default from PJSUA_MAX_CALLS.
            # Assigning a larger value succeeds in Python but native libInit
            # silently clamps it. Reject that build before binding/registering.
            if ep.uaConfig.maxCalls < required_capacity:
                raise RuntimeError("Native SIP capacity is below configured call capacity")
            ep.uaConfig.maxCalls = required_capacity
            ep.uaConfig.userAgent = "Chota-Voice-Gateway"
            ep.medConfig.clockRate = ep.medConfig.sndClockRate = 8000
            ep.medConfig.audioFramePtime, ep.medConfig.ecTailLen = 20, 0
            ep.logConfig.level = ep.logConfig.consoleLevel = 0
            ep.logConfig.msgLogging = False
            self.endpoint.libInit(ep)
            udp = pj.TransportConfig()
            udp.port = self.settings.port
            udp.boundAddress = udp.publicAddress = self.settings.bind_address
            transport_id = self.endpoint.transportCreate(pj.PJSIP_TRANSPORT_UDP, udp)
            self.endpoint.libStart()
            self.endpoint.audDevManager().setNullDev()
            for codec in self.endpoint.codecEnum2():
                self.endpoint.codecSetPriority(
                    codec.codecId,
                    255
                    if codec.codecId == "PCMA/8000/1"
                    else 254
                    if codec.codecId == "PCMU/8000/1"
                    else 0,
                )
            account_type, self.call_type, self.port_type = self._native_types()
            config = pj.AccountConfig()
            config.idUri = f"sip:{self.settings.username}@{self.settings.domain}"
            server = f"sip:{self.settings.server}:{self.settings.server_port}"
            config.regConfig.registrarUri, config.regConfig.timeoutSec = server, 300
            config.sipConfig.transportId = transport_id
            config.sipConfig.proxies.append(server + ";lr")
            config.sipConfig.authCreds.append(
                pj.AuthCredInfo(
                    "digest",
                    "*",
                    self.settings.username,
                    0,
                    self.settings.password.get_secret_value(),
                )
            )
            config.natConfig.iceEnabled = False
            config.natConfig.contactRewriteUse = config.natConfig.viaRewriteUse = (
                config.natConfig.sdpNatRewriteUse
            ) = 0
            media = config.mediaConfig.transportConfig
            media.boundAddress = media.publicAddress = self.settings.bind_address
            media.port, media.portRange = (
                self.settings.rtp_port,
                2 * (self.settings.max_calls + self.settings.max_waiting),
            )
            self.account = account_type()
            self.poll_task = asyncio.create_task(self._poll())
            self.account.create(config)
            await asyncio.wait_for(self.registration.wait(), self.settings.registration_timeout)
            if not self.registered:
                raise ConnectionError("SIP registration unavailable")
        except BaseException:
            await self.stop()
            raise

    def _native_types(self):
        pj, client = self.pj, self

        class Account(pj.Account):
            def onRegState(self, prm):
                client.registered = bool(self.getInfo().regIsActive and 200 <= prm.code < 300)
                if prm.code >= 200:
                    client.registration.set()

            def onIncomingCall(self, prm):
                client._incoming(prm.callId, prm.rdata)

        class Call(pj.Call):
            def __init__(self, call_id):
                super().__init__(client.account, call_id)
                self.transport = None
                self.confirmed = self.finished = self.running = self.session_started = False
                self.media = None

            def onCallTransferRequest(self, prm):
                prm.statusCode = 603

            def onCallReplaceRequest(self, prm):
                prm.statusCode = 603

            def onCallState(self, prm):
                state = self.getInfo().state
                if state == pj.PJSIP_INV_STATE_CONFIRMED:
                    self.confirmed = True
                    client._maybe_session(self)
                elif state == pj.PJSIP_INV_STATE_DISCONNECTED:
                    self.finished = True
                    if self.transport:
                        self.transport.remote_closed()
                    asyncio.get_running_loop().call_soon(client._retire, self)

            def onCallMediaState(self, prm):
                if not self.transport or self.finished:
                    return
                try:
                    if self.media is not None:
                        self.media.stopTransmit(self.transport.media_port)
                        self.transport.media_port.stopTransmit(self.media)
                        self.media = None
                    self.transport.audio.clear()
                    # SWIG media entries borrow storage from CallInfo. Keep its
                    # owner alive until the native media index has been used.
                    info = self.getInfo()
                    active = next(
                        (
                            m
                            for m in info.media
                            if m.type == pj.PJMEDIA_TYPE_AUDIO
                            and m.status == pj.PJSUA_CALL_MEDIA_ACTIVE
                        ),
                        None,
                    )
                    if active is None:
                        return
                    if self.transport.media_port is None:
                        self.transport.media_port = client.port_type(self.transport.audio)
                    self.media = self.getAudioMedia(active.index)
                    self.media.startTransmit(self.transport.media_port)
                    self.transport.media_port.startTransmit(self.media)
                    self.transport.ready.set()
                    client._maybe_session(self)
                except Exception:
                    logger.warning("sip_media_failed")
                    client.hangup(self)

        class Port(pj.AudioMediaPort):
            def __init__(self, buffer):
                super().__init__()
                self.buffer = buffer
                fmt = pj.MediaFormatAudio()
                fmt.type = pj.PJMEDIA_TYPE_AUDIO
                fmt.clockRate, fmt.channelCount, fmt.bitsPerSample, fmt.frameTimeUsec = (
                    8000,
                    1,
                    16,
                    20000,
                )
                self.createPort("voice-pcm", fmt)

            def onFrameRequested(self, frame):
                frame.type = pj.PJMEDIA_FRAME_TYPE_AUDIO
                frame.buf.assign_from_bytes(self.buffer.request(frame.size))

            def onFrameReceived(self, frame):
                if frame.type != pj.PJMEDIA_FRAME_TYPE_AUDIO or not frame.size:
                    return
                if frame.size > 4096 or frame.size < 0 or frame.size % 2:
                    self.buffer.receive(
                        b"x"
                    )  # Mark failure without allocating an unbounded native frame.
                    return
                pcm = bytearray(frame.size)
                frame.buf.copy_to_bytearray(pcm)
                self.buffer.receive(bytes(pcm))

        return Account, Call, Port

    async def _poll(self):
        try:
            while not self.stopping:
                self.endpoint.libHandleEvents(0)
                await asyncio.sleep(0.01)
        except Exception:
            self.registered = False
            self.registration.set()
            for call in tuple(self.calls.values()):
                if call.transport:
                    call.transport.remote_closed()
            logger.error("sip_poll_failed")

    def _incoming(self, call_id, received):
        call = self.call_type(call_id)
        self.calls[id(call)] = call
        options = self.pj.CallOpParam()
        options.opt.audioCount, options.opt.videoCount = 1, 0
        options.statusCode = 486
        try:
            if not self.stopping and self.registered:
                queued = self.capacity is not None
                # Without a queue callback the adapter keeps its fixed admission
                # bound; with one it also answers a few callers to hold them.
                limit = self.settings.max_calls + (self.settings.max_waiting if queued else 0)
                if len(self.calls) <= limit:
                    number = called_did(self.settings, received)
                    transport = SIPTransport(self, call)
                    transport.caller_number = caller_ani(received, number)
                    transport.queued = queued and not self.capacity()
                    self.registry.register(Registration(call_id=transport.call_id, number=number))
                    call.transport = transport
                    options.statusCode = 200
        except Exception:
            options.statusCode = 403
        try:
            call.answer(options)
        except Exception:
            if call.transport:
                call.transport.remote_closed()
            call.finished = True
            self._retire(call)

    def _maybe_session(self, call):
        if (
            call.finished
            or call.session_started
            or not call.confirmed
            or not call.transport
            or not call.transport.ready.is_set()
        ):
            return
        call.session_started = call.running = True
        task = asyncio.create_task(self._run_session(call))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def _run_session(self, call):
        try:
            if call.transport.queued:
                await self._serve_queued(call.transport)
            else:
                await self.handle_transport(call.transport)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("sip_session_failed")
        finally:
            await call.transport.close()
            call.running = False
            if call.finished:
                self._retire(call)

    async def _serve_queued(self, transport):
        """Warn a caller that every session is busy, then wait for a free slot.

        The caller hears the announcement and stays on the line; as soon as a
        session ends they are handed to the normal authorized conversation.
        """
        awaiting = self.capacity is None or not self.capacity()
        if awaiting and self.announce is not None:
            await self.announce(transport, BUSY_MESSAGE)
        deadline = time.monotonic() + self.settings.busy_hold_seconds
        while time.monotonic() < deadline:
            if not await transport.drain_audio():
                return  # Caller hung up while waiting.
            if self.capacity is None or self.capacity():
                break
            await asyncio.sleep(0.2)
        if self.capacity is None or self.capacity():
            logger.info("sip_queued_call_admitted")
            await self.handle_transport(transport)
            return
        if self.announce is not None:
            await self.announce(transport, BUSY_TIMEOUT_MESSAGE)
        logger.info("sip_queued_call_timed_out")

    def hangup(self, call):
        if call.finished:
            return
        try:
            options = self.pj.CallOpParam()
            options.statusCode = 603
            call.hangup(options)
        except Exception:
            logger.warning("sip_hangup_failed")

    def _retire(self, call):
        if call.running:
            return
        if call.transport and call.transport.media_port:
            if call.media:
                try:
                    call.media.stopTransmit(call.transport.media_port)
                    call.transport.media_port.stopTransmit(call.media)
                except Exception:
                    pass
            call.media = None
            call.transport.media_port = None
        self.calls.pop(id(call), None)
        if call.transport:
            call.transport.call = None

    async def stop(self):
        if self.close_task is None:
            self.close_task = asyncio.create_task(self._stop())
        await finish_owned_task(self.close_task)

    async def _stop(self):
        self.stopping = True
        for call in tuple(self.calls.values()):
            self.hangup(call)
            if call.transport:
                call.transport.remote_closed()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            _, pending = await asyncio.wait(tuple(self.tasks), timeout=8)
            if pending:
                raise RuntimeError("SIP sessions did not drain; endpoint retained")
        if self.poll_task:
            self.poll_task.cancel()
            await asyncio.gather(self.poll_task, return_exceptions=True)
        for call in tuple(self.calls.values()):
            self._retire(call)
        call = None
        self.registered = False
        try:
            if self.account:
                try:
                    self.account.shutdown()
                finally:
                    self.account = None
        finally:
            gc.collect()
            if self.endpoint:
                try:
                    self.endpoint.libDestroy()
                finally:
                    self.endpoint = None
