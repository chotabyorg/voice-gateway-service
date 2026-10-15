"""Native PJSUA2 acceptance on loopback only; no carrier, credentials or AI.

Run with the service's Python and native binding: python scripts/sip_loopback.py.
The synthetic UDP registrar/caller and RTP peer all bind 127.0.0.1. Nothing is
recorded; only counters and lifecycle outcomes are printed.
"""

import asyncio
import json
import re
import socket
import struct
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from voice_gateway.sip import SIPClient, SIPSettings  # noqa: E402


class Peer(asyncio.DatagramProtocol):
    def __init__(self):
        self.messages = asyncio.Queue(maxsize=1024)
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if data.startswith(b"REGISTER ") or data.startswith(b"BYE "):
            message = data.decode("ascii")
            headers = []
            for name in ("Via", "From", "To", "Call-ID", "CSeq", "Contact"):
                values = re.findall(rf"^{name}: (.+)$", message, re.M)
                headers.extend(f"{name}: {value.strip()}" for value in values)
            response = "\r\n".join(
                ["SIP/2.0 200 OK", *headers, "Expires: 300", "Content-Length: 0", "", ""]
            )
            self.transport.sendto(response.encode(), addr)
        else:
            self.messages.put_nowait((data, addr))

    @property
    def port(self):
        return self.transport.get_extra_info("sockname")[1]

    async def response(self, method):
        async with asyncio.timeout(5):
            while True:
                data, addr = await self.messages.get()
                text = data.decode("ascii")
                if f" {method}\r\n" in text and text.startswith("SIP/2.0 200"):
                    return text, addr


def unused_port(*, even=False):
    for _ in range(100):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
            if port < 64000 and (not even or port % 2 == 0):
                return port
    raise RuntimeError("No loopback port available")


class Registry:
    def __init__(self):
        self.count = 0

    def register(self, registration):
        assert registration.number == "+375170000000"
        self.count += 1


def call_diagnostic(call):
    info = call.getInfo()
    return {
        "confirmed": call.confirmed,
        "media_ready": call.transport.ready.is_set(),
        "native_media": call.media is not None,
        "media_states": [(m.type, m.status, m.index) for m in info.media],
        "buffer_failed": call.transport.audio.failed,
    }


async def main():
    loop = asyncio.get_running_loop()
    _, sip = await loop.create_datagram_endpoint(Peer, local_addr=("127.0.0.1", 0))
    _, rtp = await loop.create_datagram_endpoint(Peer, local_addr=("127.0.0.1", 0))
    config = SIPSettings(
        server="127.0.0.1",
        server_port=sip.port,
        domain="127.0.0.1",
        username="loopback",
        password="synthetic-only",
        bind_address="127.0.0.1",
        port=unused_port(),
        rtp_port=unused_port(even=True),
        registration_timeout=5,
        media_idle_timeout=5,
    )
    received_pcm = bytearray()
    session_ended = asyncio.Event()

    async def handle(transport):
        try:
            await transport.prepare()
            while True:
                pcm = await transport.read_audio_chunk()
                if pcm is None:
                    break
                received_pcm.extend(pcm)
                assert len(received_pcm) <= 64000
                await transport.write_audio_chunk(b"\xb0\x04" * (len(pcm) // 2))
        finally:
            session_ended.set()

    registry = Registry()
    client = SIPClient(config, handle, registry)
    try:
        await client.start()
        assert client.registered
        call_id, tag = str(uuid4()), uuid4().hex
        uri = f"sip:+375170000000@127.0.0.1:{config.port}"
        target = ("127.0.0.1", config.port)
        to = f"<{uri}>"

        def request(method, cseq, body="", target_uri=uri):
            headers = [
                f"{method} {target_uri} SIP/2.0",
                f"Via: SIP/2.0/UDP 127.0.0.1:{sip.port};branch=z9hG4bK{uuid4().hex}",
                f"From: <sip:synthetic@127.0.0.1>;tag={tag}",
                f"To: {to}",
                f"Call-ID: {call_id}",
                f"CSeq: {cseq} {method}",
                f"Contact: <sip:synthetic@127.0.0.1:{sip.port}>",
                "Max-Forwards: 70",
                *(["Content-Type: application/sdp"] if body else []),
                f"Content-Length: {len(body)}",
                "",
                body,
            ]
            sip.transport.sendto("\r\n".join(headers).encode(), target)

        sdp = "\r\n".join(
            [
                "v=0",
                "o=- 1 1 IN IP4 127.0.0.1",
                "s=synthetic",
                "c=IN IP4 127.0.0.1",
                "t=0 0",
                f"m=audio {rtp.port} RTP/AVP 8 0",
                "a=rtpmap:8 PCMA/8000",
                "a=rtpmap:0 PCMU/8000",
                "a=sendrecv",
                "",
            ]
        )
        request("INVITE", 1, sdp)
        answer, _ = await sip.response("INVITE")
        to = re.search(r"^To: (.+)$", answer, re.M)[1].strip()
        contact = re.search(r"^Contact: <([^>]+)>", answer, re.M)[1]
        media_port = int(re.search(r"^m=audio (\d+) RTP/AVP", answer, re.M)[1])
        request("ACK", 1, target_uri=contact)
        for sequence in range(100):
            packet = struct.pack("!BBHII", 0x80, 8, sequence, sequence * 160, 123456)
            rtp.transport.sendto(packet + b"\x80" * 160, ("127.0.0.1", media_port))
            await asyncio.sleep(0.02)
        packets = []
        while not rtp.messages.empty():
            packets.append(rtp.messages.get_nowait()[0])
        if not received_pcm or not any(received_pcm):
            print(
                json.dumps(
                    {
                        "diagnostic": "no_inbound_audio",
                        "rtp_packets": len(packets),
                        "sessions": len(client.tasks),
                        "pcm_bytes": len(received_pcm),
                        "calls": [call_diagnostic(c) for c in client.calls.values()],
                    }
                )
            )
        assert received_pcm and any(received_pcm), "No decoded inbound PCM"
        assert any(
            len(packet) >= 172 and packet[1] & 0x7F == 8 and any(b != 0xD5 for b in packet[12:])
            for packet in packets
        ), "No encoded outbound audio"
        request("BYE", 2, target_uri=contact)
        await sip.response("BYE")
        await asyncio.wait_for(session_ended.wait(), 3)
        async with asyncio.timeout(3):
            while client.calls or client.tasks:
                await asyncio.sleep(0.01)
        assert registry.count == 1
        print(
            json.dumps(
                {
                    "sip_loopback": "passed",
                    "inbound_pcm_bytes": len(received_pcm),
                    "outbound_rtp_packets": len(packets),
                    "sessions_remaining": 0,
                }
            )
        )
    finally:
        await client.stop()
        sip.transport.close()
        rtp.transport.close()


if __name__ == "__main__":
    asyncio.run(main())
