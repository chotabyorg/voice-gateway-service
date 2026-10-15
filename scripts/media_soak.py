"""Native loopback media acceptance. Synthetic audio/providers; no Docker or API keys.

Run from the repository: python -m scripts.media_soak --rounds 120 --concurrency 8
"""

import argparse
import asyncio
import gc
import json
import logging
import math
import os
import platform
import struct
import time
import tracemalloc
import uuid
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path

from voice_gateway.application import LocalFakeConversationPort, VoiceGatewayRuntime
from voice_gateway.events import PlaybackState
from voice_gateway.media.batch_stt import BatchSTT
from voice_gateway.media.interfaces import BaseTTS
from voice_gateway.settings import Settings


def pcm(amplitude, milliseconds):
    return struct.pack("<h", amplitude) * (8 * milliseconds)


def frame(kind, payload=b""):
    return struct.pack("!BH", kind, len(payload)) + payload


def distribution(values):
    values = sorted(values)
    if not values:
        return {"samples": 0}
    return {
        "samples": len(values),
        "p50_ms": round(values[math.ceil(len(values) * 0.5) - 1], 2),
        "p95_ms": round(values[math.ceil(len(values) * 0.95) - 1], 2),
        "max_ms": round(values[-1], 2),
    }


def native_resources():
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        kernel.GetProcessHandleCount.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        count = wintypes.DWORD()
        if not kernel.GetProcessHandleCount(kernel.GetCurrentProcess(), ctypes.byref(count)):
            raise ctypes.WinError(ctypes.get_last_error())
        return count.value
    if Path("/proc/self/fd").is_dir():
        return len(os.listdir("/proc/self/fd"))
    return None


def task_inventory():
    # Windows replaces its listener accept task after each accepted connection.
    # Compare coroutine counts, not object identities; per-call tasks must drain.
    return Counter(task.get_coro().__qualname__ for task in asyncio.all_tasks())


class Metrics(logging.Handler):
    def __init__(self):
        super().__init__()
        self.counts = Counter()
        self.timings = {}

    def emit(self, record):
        event = getattr(record, "event", None)
        if event:
            self.counts[event] += 1
        if event in {"media_first_audio", "media_response_gap", "media_interrupt_timing"}:
            self.timings.setdefault(event, deque(maxlen=4096)).append(record.duration_ms)


class Providers:
    def __init__(self):
        self.created = Counter()
        self.closed = Counter()

    def stt(self):
        owner = self
        owner.created["stt"] += 1

        class STT(BatchSTT):
            async def _transcribe(self, audio):
                await asyncio.sleep(0.03)
                return "synthetic utterance"

            async def _close(self):
                owner.closed["stt"] += 1

        return STT()

    def tts(self):
        owner = self
        owner.created["tts"] += 1

        class TTS(BaseTTS):
            async def synthesize(self, text):
                await asyncio.sleep(0.04)
                return pcm(1000, 200)

            async def close(self):
                owner.closed["tts"] += 1

        return TTS()


class Port(LocalFakeConversationPort):
    def __init__(self):
        self.outcomes = Counter()
        self.finished = {}

    async def call_started(self, event):
        self.finished[event.call_id] = asyncio.Event()

    async def playback_fact(self, event):
        if event.state in {PlaybackState.COMPLETED, PlaybackState.INTERRUPTED}:
            self.outcomes[event.state.value] += 1
            self.finished[event.call_id].set()

    async def call_ended(self, event):
        self.finished.pop(event.call_id, None)


async def settled(runtime):
    async with asyncio.timeout(6):
        while runtime.status().active_calls:
            await asyncio.sleep(0.005)
    # Allow task done callbacks and OS pipe/socket completions to retire.
    await asyncio.sleep(0.02)


async def one_call(port, seam, index, latencies, intervals, outcomes):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    scenario = index % 12
    try:
        if scenario == 0:
            writer.write(b"\x01")  # Disconnect in the middle of the handshake header.
            outcomes["truncated_handshake"] += 1
            return
        identity = uuid.uuid4()
        wire = frame(1, identity.bytes)
        for part in (wire[:1], wire[1:5], wire[5:]):
            writer.write(part)
            await writer.drain()
            await asyncio.sleep(0)
        audio = pcm(1200, 120) + pcm(0, 500)
        size = (126, 320, 962, 4096)[index % 4]
        deadline = time.perf_counter()
        for start in range(0, len(audio), size):
            chunk = audio[start : start + size]
            # Approximate sample-time delivery plus a periodic 25 ms producer stall.
            deadline += len(chunk) / 16000
            await asyncio.sleep(max(0, deadline - time.perf_counter()))
            if start and start // size == 4:
                await asyncio.sleep(0.025)
                deadline = time.perf_counter()
            writer.write(frame(0x10, chunk))
            await writer.drain()
        end_of_input = time.perf_counter()
        if scenario == 1:
            outcomes["disconnect_before_response"] += 1
            return
        received, previous = 0, None
        async with asyncio.timeout(5):
            while received < 3200:
                kind, length = struct.unpack("!BH", await reader.readexactly(3))
                payload = await reader.readexactly(length)
                assert kind == 0x10 and length == 320 and len(payload) == length
                now = time.perf_counter()
                if previous is None:
                    latencies.append((now - end_of_input) * 1000)
                else:
                    intervals.append((now - previous) * 1000)
                previous = now
                received += length
                if scenario == 2:
                    writer.write(frame(0x10, pcm(2400, 160)))
                    await writer.drain()
                    await seam.finished[str(identity)].wait()
                    outcomes["barge_in"] += 1
                    break
            else:
                # Last-byte receipt precedes the final paced frame's completion.
                # Wait for its local fact, rather than racing hangup against a sleep.
                await seam.finished[str(identity)].wait()
                outcomes["completed"] += 1
            writer.write(frame(0))
            await writer.drain()
            await reader.read()
    finally:
        writer.close()
        await writer.wait_closed()


async def run(rounds, concurrency):
    providers, port, metrics = Providers(), Port(), Metrics()
    logger = logging.getLogger("voice_gateway")
    old_level = logger.level
    logger.addHandler(metrics)
    logger.setLevel(logging.INFO)
    runtime = VoiceGatewayRuntime(
        Settings(
            _env_file=None,
            mode="local_fake",
            audiosocket_port=0,
            max_calls=concurrency,
            max_call_seconds=15,
            audiosocket_idle_timeout_seconds=2,
        ),
        stt_factory=providers.stt,
        tts_factory=providers.tts,
        conversation_port=port,
    )
    latencies, intervals = deque(maxlen=4096), deque(maxlen=4096)
    outcomes = Counter()
    started = time.perf_counter()
    tracemalloc.start()
    samples = []
    try:
        await runtime.start()
        # Warm up the event loop, logging, socket and provider allocations.
        await asyncio.gather(
            *(one_call(runtime.bound_port, port, 3, [], [], Counter()) for _ in range(concurrency))
        )
        await settled(runtime)
        baseline_tasks = task_inventory()
        baseline_native = native_resources()
        for iteration in range(rounds):
            await asyncio.gather(
                *(
                    one_call(
                        runtime.bound_port,
                        port,
                        iteration * concurrency + i,
                        latencies,
                        intervals,
                        outcomes,
                    )
                    for i in range(concurrency)
                )
            )
            await settled(runtime)
            assert providers.created == providers.closed, "Provider resources were not released"
            leaked = task_inventory() - baseline_tasks
            assert not leaked, "Per-call tasks leaked: " + repr(leaked)
            gc.collect()
            samples.append(tracemalloc.get_traced_memory()[0])
        final_native = native_resources()
        # Verify runtime shutdown with live sockets, rather than an empty listener only.
        live = [
            await asyncio.open_connection("127.0.0.1", runtime.bound_port)
            for _ in range(concurrency)
        ]
        try:
            for _, writer in live:
                writer.write(frame(1, uuid.uuid4().bytes))
                await writer.drain()
            await asyncio.sleep(0.05)
            shutdown_at = time.perf_counter()
            await runtime.stop()
            shutdown_ms = (time.perf_counter() - shutdown_at) * 1000
            for reader, _ in live:
                assert await asyncio.wait_for(reader.read(), 1) == b""
        finally:
            for _, writer in live:
                writer.close()
                await writer.wait_closed()
        assert providers.created == providers.closed and not runtime.status().active_calls
        assert port.outcomes["completed"] == outcomes["completed"] + concurrency, (
            port.outcomes,
            outcomes,
        )
        assert port.outcomes["interrupted"] == outcomes["barge_in"], (port.outcomes, outcomes)
        assert not metrics.counts["media_cleanup_timeout"]
        assert not metrics.counts["voice_shutdown_tasks_pending"]
        _, peak = tracemalloc.get_traced_memory()
        return {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": "native loopback; synthetic PCM, STT 30ms, TTS 40ms; no external services",
            "platform": platform.platform(),
            "python": platform.python_version(),
            "rounds": rounds,
            "concurrency": concurrency,
            "calls": rounds * concurrency,
            "elapsed_seconds": round(time.perf_counter() - started, 2),
            "outcomes": dict(outcomes),
            "providers_created": dict(providers.created),
            "providers_closed": dict(providers.closed),
            "active_calls_after_shutdown": 0,
            "leaked_call_tasks": 0,
            "native_resources_after_warmup": baseline_native,
            "native_resources_after_soak": final_native,
            "shutdown_with_live_calls_ms": round(shutdown_ms, 2),
            "allocated_bytes_first_round": samples[0],
            "allocated_bytes_last_round": samples[-1],
            "allocated_bytes_peak": peak,
            "allocated_bytes_final_quarter_range": max(samples[3 * rounds // 4 :])
            - min(samples[3 * rounds // 4 :]),
            "input_end_to_first_received_audio": distribution(latencies),
            "received_frame_intervals": distribution(intervals),
            "media_timings": {key: distribution(value) for key, value in metrics.timings.items()},
            "event_counts": dict(metrics.counts),
        }
    finally:
        await runtime.stop()
        tracemalloc.stop()
        logger.removeHandler(metrics)
        logger.setLevel(old_level)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=120)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 1 <= args.rounds <= 1000 or not 1 <= args.concurrency <= 128:
        parser.error("rounds must be 1..1000; concurrency must be 1..128")
    report = asyncio.run(run(args.rounds, args.concurrency))
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
