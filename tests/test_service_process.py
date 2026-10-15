"""Local OS-process integration using deterministic fakes and the AudioSocket wire."""

import asyncio
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import time
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import pytest


ROOT = os.path.dirname(os.path.dirname(__file__))


def free_port():
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return server.getsockname()[1]


def get_http(url):
    try:
        with urlopen(url, timeout=1) as response:
            return response.status, response.read()
    except HTTPError as error:
        return error.code, error.read()


async def launch(mode, audio_port=0):
    http_port = free_port()
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": ROOT,
            "VOICE_MODE": mode,
            "VOICE_HTTP_HOST": "127.0.0.1",
            "VOICE_HTTP_PORT": str(http_port),
            "VOICE_AUDIOSOCKET_HOST": "127.0.0.1",
            "VOICE_AUDIOSOCKET_PORT": str(audio_port),
            "VOICE_MAX_CALL_SECONDS": "30",
            "VOICE_SHUTDOWN_TIMEOUT_SECONDS": "4",
        }
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "voice_gateway",
        cwd=ROOT,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    lines = []

    async def collect():
        while True:
            line = await process.stdout.readline()
            if not line:
                return
            lines.append(line.decode(errors="replace").rstrip())

    collector = asyncio.create_task(collect())
    base = f"http://127.0.0.1:{http_port}"
    return process, collector, lines, base


async def wait_http(url, expected):
    async with asyncio.timeout(10):
        while True:
            try:
                status, body = await asyncio.to_thread(get_http, url)
                if status == expected:
                    return body
            except (OSError, URLError):
                pass
            await asyncio.sleep(0.05)


async def stop(process, collector, lines):
    if process.returncode is None:
        process.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
    try:
        async with asyncio.timeout(10):
            result = await process.wait()
            await collector
    except TimeoutError as exc:
        process.kill()
        await process.wait()
        await collector
        raise AssertionError("service shutdown hung:\n" + "\n".join(lines)) from exc
    assert any("Application shutdown complete" in line for line in lines), lines
    # Uvicorn restores then re-raises SIGBREAK; Windows CRT may exit with 3.
    # Accept this only after the application has confirmed graceful shutdown.
    expected = (0, 3, 0xC000013A) if os.name == "nt" else (0, -signal.SIGTERM)
    assert result in expected, f"exit={result}\n" + "\n".join(lines)


def wire_frame(kind, payload=b""):
    return struct.pack("!BH", kind, len(payload)) + payload


async def fake_audio_call(port, *, speak=False):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(wire_frame(1, uuid.uuid4().bytes))
    if speak:
        for _ in range(20):
            writer.write(wire_frame(0x10, struct.pack("<h", 1200) * 160))
        for _ in range(25):
            writer.write(wire_frame(0x10, bytes(320)))
        await writer.drain()
        audio_frames = 0
        async with asyncio.timeout(6):
            while audio_frames < 5:
                kind, size = struct.unpack("!BH", await reader.readexactly(3))
                payload = await reader.readexactly(size)
                if kind == 0x10:
                    assert len(payload) == 320
                    audio_frames += 1
        assert audio_frames == 5
        time.sleep(0.1)  # Allow the final paced frame to produce its completion fact.
    writer.write(wire_frame(0))
    await writer.drain()
    writer.close()
    await writer.wait_closed()


async def wait_log(lines, predicate):
    try:
        async with asyncio.timeout(5):
            while not any(predicate(line) for line in lines):
                await asyncio.sleep(0.01)
    except TimeoutError as exc:
        raise AssertionError("Expected process log was missing:\n" + "\n".join(lines)) from exc


@pytest.mark.asyncio
async def test_service_is_inert_by_default_and_restarts_cleanly():
    unbound_port = free_port()
    process, collector, lines, base = await launch("disabled", audio_port=unbound_port)
    try:
        await wait_http(base + "/health", 200)
        status, _ = await asyncio.to_thread(get_http, base + "/ready")
        assert status == 503
        status, _ = await asyncio.to_thread(get_http, base + "/docs")
        assert status == 404
        with pytest.raises(OSError):
            await asyncio.open_connection("127.0.0.1", unbound_port)
        assert any("voice_runtime_inert" in line for line in lines)
    finally:
        await stop(process, collector, lines)

    restarted, restarted_collector, restarted_lines, restarted_base = await launch("disabled")
    try:
        await wait_http(restarted_base + "/health", 200)
        status, _ = await asyncio.to_thread(get_http, restarted_base + "/ready")
        assert status == 503
    finally:
        await stop(restarted, restarted_collector, restarted_lines)


@pytest.mark.asyncio
async def test_fake_tcp_call_vad_stt_tts_playback_and_repeated_disconnect():
    process, collector, lines, base = await launch("local_fake")
    active_reader = active_writer = None
    try:
        ready = json.loads(await wait_http(base + "/ready", 200))
        port = ready["audiosocket_port"]
        await fake_audio_call(port, speak=True)
        await wait_log(
            lines,
            lambda line: '"message":"voice_playback_fact"' in line
            and '"playback_state":"completed"' in line,
        )

        utterances = [line for line in lines if '"message":"voice_final_utterance"' in line]
        assert len(utterances) == 1, lines
        for _ in range(3):
            await fake_audio_call(port)
        async with asyncio.timeout(3):
            while sum('"message":"voice_call_ended"' in line for line in lines) < 4:
                await asyncio.sleep(0.01)
        assert sum('"message":"voice_call_ended"' in line for line in lines) == 4

        active_reader, active_writer = await asyncio.open_connection("127.0.0.1", port)
        active_writer.write(wire_frame(1, uuid.uuid4().bytes))
        await active_writer.drain()
        await wait_log(
            lines,
            lambda line: '"message":"media_call_state"' in line and '"state":"active"' in line,
        )
    finally:
        await stop(process, collector, lines)
        assert sum('"message":"voice_call_ended"' in line for line in lines) == 5
        assert active_reader is not None and active_writer is not None
        assert await active_reader.read() == b""
        active_writer.close()
        await active_writer.wait_closed()

    restarted, restarted_collector, restarted_lines, restarted_base = await launch("local_fake")
    try:
        ready = json.loads(await wait_http(restarted_base + "/ready", 200))
        assert ready["mode"] == "local_fake"
    finally:
        await stop(restarted, restarted_collector, restarted_lines)
