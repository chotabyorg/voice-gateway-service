"""Real PJSUA2/RTP capacity with a synthetic PBX bound only to loopback."""

import asyncio
import ctypes
import json
import re
import struct
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.sip_loopback import Peer, Registry, unused_port
from voice_gateway.sip import SIPClient, SIPSettings

progress = {"stage": "setup", "admitted": 0}


async def main(count=5, waiting=2):
    loop = asyncio.get_running_loop()
    _, sip = await loop.create_datagram_endpoint(Peer, local_addr=("127.0.0.1", 0))
    config = SIPSettings(
        server="127.0.0.1",
        server_port=sip.port,
        domain="127.0.0.1",
        username="loopback",
        password="synthetic-only",
        bind_address="127.0.0.1",
        port=unused_port(),
        rtp_port=unused_port(even=True),
        max_calls=count,
        max_waiting=waiting,
        registration_timeout=5,
        media_idle_timeout=8,
        input_gain=1,
    )
    received = {}

    async def handle(transport):
        received[transport.call_id] = dict(bytes=0, nonzero=False)
        try:
            await transport.prepare()
            while True:
                pcm = await transport.read_audio_chunk()
                if pcm is None:
                    break
                received[transport.call_id]["bytes"] += len(pcm)
                received[transport.call_id]["nonzero"] |= any(pcm)
                await transport.write_audio_chunk(b"\xb0\x04" * (len(pcm) // 2))
        finally:
            await transport.close()

    registry = Registry()
    client = SIPClient(config, handle, registry)
    peers = []
    calls = []
    peak = 0
    try:
        progress["stage"] = "registration"
        print("SIP_PROGRESS=" + json.dumps(progress), flush=True)
        await client.start()
        assert client.registered
        library = ctypes.CDLL(client.pj._pjsua2.__file__)
        maximum = library.pjsua_call_get_max_count
        maximum.restype = ctypes.c_uint
        maximum.argtypes = []
        progress["native_max_calls"] = maximum()
        print("SIP_PROGRESS=" + json.dumps(progress), flush=True)
        assert maximum() >= count + waiting
        for index in range(count):
            progress["stage"] = "invite"
            _, rtp = await loop.create_datagram_endpoint(Peer, local_addr=("127.0.0.1", 0))
            peers.append(rtp)
            call_id, tag = str(uuid4()), uuid4().hex
            uri = f"sip:+375170000000@127.0.0.1:{config.port}"
            call = {"id": call_id, "tag": tag, "uri": uri, "to": f"<{uri}>", "rtp": rtp}

            def request(method, cseq, body="", *, call=call):
                target_uri = call.get("contact", call["uri"])
                headers = [
                    f"{method} {target_uri} SIP/2.0",
                    f"Via: SIP/2.0/UDP 127.0.0.1:{sip.port};branch=z9hG4bK{uuid4().hex}",
                    f"From: <sip:synthetic@127.0.0.1>;tag={call['tag']}",
                    f"To: {call['to']}",
                    f"Call-ID: {call['id']}",
                    f"CSeq: {cseq} {method}",
                    f"Contact: <sip:synthetic@127.0.0.1:{sip.port}>",
                    "Max-Forwards: 70",
                    *(["Content-Type: application/sdp"] if body else []),
                    f"Content-Length: {len(body)}",
                    "",
                    body,
                ]
                sip.transport.sendto("\r\n".join(headers).encode(), ("127.0.0.1", config.port))

            call["request"] = request
            sdp = "\r\n".join(
                [
                    "v=0",
                    f"o=- {index + 1} 1 IN IP4 127.0.0.1",
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
            assert f"Call-ID: {call_id}" in answer
            call["to"] = re.search(r"^To: (.+)$", answer, re.M)[1].strip()
            call["contact"] = re.search(r"^Contact: <([^>]+)>", answer, re.M)[1]
            call["media_port"] = int(re.search(r"^m=audio (\d+) RTP/AVP", answer, re.M)[1])
            request("ACK", 1)
            calls.append(call)
            progress["admitted"] = len(calls)
            print("SIP_PROGRESS=" + json.dumps(progress), flush=True)
            peak = max(peak, len(client.calls))
        assert peak == count and registry.count == count and len(client.calls) == count
        progress["stage"] = "rtp"
        for sequence in range(120):
            for index, call in enumerate(calls):
                packet = struct.pack("!BBHII", 0x80, 8, sequence, sequence * 160, 123456 + index)
                call["rtp"].transport.sendto(
                    packet + b"\xd7" * 160, ("127.0.0.1", call["media_port"])
                )
            await asyncio.sleep(0.02)
        progress["stage"] = "audio_check"
        inbound = [data["bytes"] for data in received.values() if data["nonzero"]]
        assert len(inbound) == count
        outbound = []
        for call in calls:
            packets = []
            while not call["rtp"].messages.empty():
                packets.append(call["rtp"].messages.get_nowait()[0])
            useful = sum(
                len(p) >= 172 and p[1] & 0x7F == 8 and any(b != 0xD5 for b in p[12:])
                for p in packets
            )
            assert useful > 0
            outbound.append(useful)
            call["request"]("BYE", 2)
            await sip.response("BYE")
        async with asyncio.timeout(5):
            while client.calls or client.tasks:
                await asyncio.sleep(0.01)
        return dict(
            passed=True,
            kind="LOOPBACK_REAL_PJSUA2_RTP_SYNTHETIC_PBX_NO_PROVIDERS",
            concurrent_calls=count,
            peak_native_calls=peak,
            inbound_pcm_bytes=inbound,
            useful_outbound_packets=outbound,
            sessions_remaining=0,
            tasks_remaining=0,
        )
    finally:
        await client.stop()
        sip.transport.close()
        for peer in peers:
            peer.transport.close()


if __name__ == "__main__":
    try:
        result = asyncio.run(main(int(sys.argv[1]), int(sys.argv[2])))
    except Exception as exc:
        result = dict(passed=False, **progress, error_type=type(exc).__name__)
        status = getattr(exc, "status", None)
        if isinstance(status, int):
            result["native_status"] = status
    print("SIP_CAPACITY_RESULT=" + json.dumps(result))
    raise SystemExit(0 if result["passed"] else 1)
