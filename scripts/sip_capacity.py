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
    """Hold real local SIP calls, reject overflow, and verify bidirectional RTP."""
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
        """Consume PCM and return synthetic audio without retaining recordings."""
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

            def request(method, cseq, body="", *, call=call, transaction_ack=False):
                """Send one synthetic SIP request with the appropriate transaction branch."""
                target_uri = call.get("contact", call["uri"])
                branch = call["invite_branch"] if transaction_ack else "z9hG4bK" + uuid4().hex
                if method == "INVITE":
                    call["invite_branch"] = branch
                headers = [
                    f"{method} {target_uri} SIP/2.0",
                    f"Via: SIP/2.0/UDP 127.0.0.1:{sip.port};branch={branch}",
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
        progress.update(peak=peak, registered_calls=registry.count, native_calls=len(client.calls))
        assert peak == count and registry.count == count and len(client.calls) == count
        # An extra caller must receive a finite busy response, never abort the
        # native process or displace an admitted call. Exercise both the app
        # limit (waiting > 0) and the configured native limit (waiting == 0).
        progress["stage"] = "overflow"
        extra = dict(call, id=str(uuid4()), tag=uuid4().hex, to=f"<{uri}>")
        extra.pop("contact", None)
        request("INVITE", 1, sdp, call=extra)
        async with asyncio.timeout(5):
            while True:
                data, _ = await sip.messages.get()
                response = data.decode("ascii")
                if f"Call-ID: {extra['id']}" in response and response.startswith("SIP/2.0 "):
                    code = int(response.split()[1])
                    if code >= 200:
                        break
        progress["overflow_status"] = code
        assert code == 486
        extra["to"] = re.search(r"^To: (.+)$", response, re.M)[1].strip()
        request("ACK", 1, call=extra, transaction_ack=True)
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
        progress["nonzero_inbound_sessions"] = len(inbound)
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
            progress["useful_outbound_packets"] = outbound.copy()
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
            overflow_status=code,
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
