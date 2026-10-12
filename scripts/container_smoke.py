"""Smoke a local-fake container over loopback without external services."""

import json
import socket
import struct
import sys
import time
import urllib.error
import urllib.request
import uuid


def request(path):
    with urllib.request.urlopen(path, timeout=1) as response:
        return response.status, response.read()


def wait_for_ready(base):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            status, body = request(base + "/ready")
            if status == 200:
                return json.loads(body)
        except (OSError, urllib.error.URLError):
            time.sleep(0.1)
    raise RuntimeError("container did not become ready")


def frame(kind, payload=b""):
    return struct.pack("!BH", kind, len(payload)) + payload


def run(base, audiosocket_port):
    ready = wait_for_ready(base)
    assert ready["mode"] == "local_fake"
    if len(sys.argv) > 3 and sys.argv[3] == "health-only":
        assert request(base + "/health")[0] == 200
        return
    assert request(base + "/health")[0] == 200
    with socket.create_connection(("127.0.0.1", audiosocket_port), timeout=5) as client:
        client.settimeout(6)
        client.sendall(frame(1, uuid.uuid4().bytes))
        for _ in range(20):
            client.sendall(frame(0x10, struct.pack("<h", 1200) * 160))
        for _ in range(25):
            client.sendall(frame(0x10, bytes(320)))
        audio_frames = 0
        while audio_frames < 5:
            header = read_exact(client, 3)
            kind, length = struct.unpack("!BH", header)
            payload = read_exact(client, length)
            if kind == 0x10:
                assert length == 320 and payload
                audio_frames += 1
        client.sendall(frame(0))
    assert audio_frames == 5


def read_exact(sock, size):
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise RuntimeError("container closed before playback completed")
        chunks.extend(chunk)
    return bytes(chunks)


if __name__ == "__main__":
    host_http = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:18000"
    host_audio = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1:15585"
    run("http://" + host_http, int(host_audio.rsplit(":", 1)[1]))
