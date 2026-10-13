"""Bounded, paced PCM playback with acoustic gain control and stream cleanup."""

import asyncio
import sys
import time
from array import array
from contextlib import aclosing
from dataclasses import dataclass
from .audio import PCM, samples, validate_pcm


class PlaybackControl:
    """Local gain/pause mechanics. The host retains all response authority."""

    def __init__(self):
        self.gain = self.target = 1.0
        self.remaining = 0
        self.pausing = self.stopping = False
        self.ready = asyncio.Event()
        self.ready.set()

    def ramp(self, target, milliseconds=40):
        if not 0 <= target <= 1 or milliseconds < 0:
            raise ValueError("Invalid playback gain envelope")
        self.target = target
        self.remaining = max(1, int(PCM.sample_rate * milliseconds / 1000))
        self.ready.set()

    def duck(self):
        if not self.stopping:
            self.ramp(0.2, 80)

    def pause(self):
        if not self.stopping:
            self.pausing = True
            self.ramp(0)

    def resume(self):
        if not self.stopping:
            self.pausing = False
            self.ramp(1)

    def stop(self):
        self.stopping, self.pausing = True, False
        self.ramp(0)

    @property
    def finished(self):
        return self.stopping and self.gain == 0

    def apply(self, chunk):
        if self.gain == self.target == 1:
            return chunk
        output = array("h")
        for sample in samples(chunk):
            if self.remaining:
                self.gain += (self.target - self.gain) / self.remaining
                self.remaining -= 1
                if not self.remaining:
                    self.gain = self.target
            output.append(round(sample * self.gain))
        if self.pausing and self.gain == 0:
            self.ready.clear()
        if sys.byteorder != "little":
            output.byteswap()
        return output.tobytes()


@dataclass(frozen=True)
class PlaybackResult:
    sent_bytes: int
    complete: bool


async def play_pcm(audio, write, **kwargs):
    validate_pcm(audio)

    async def chunks():
        for offset in range(0, len(audio), 65536):
            yield audio[offset : offset + 65536]

    return await play_pcm_stream(chunks(), write, **kwargs)


async def play_pcm_stream(
    chunks,
    write,
    *,
    on_frame=None,
    should_continue=lambda: True,
    clock=time.monotonic,
    sleep=asyncio.sleep,
    control=None,
    emit=None,
    max_bytes=8 * 1024 * 1024,
    max_chunk_bytes=65536,
):
    """Reframe network chunks, including split samples, into paced 20 ms PCM.

    Only one input chunk plus a partial frame is retained. Slow I/O or provider
    stalls reset pacing, so a delayed stream cannot burst frames to catch up.
    """
    deadline, sent, received = clock(), 0, 0
    pending = bytearray()

    async def deliver(chunk):
        nonlocal deadline, sent
        if control:
            paused = not control.ready.is_set()
            await control.ready.wait()
            if paused:
                deadline = clock()
            if control.finished:
                return False
        if not should_continue():
            return False
        now = clock()
        if now > deadline + PCM.frame_ms / 1000:
            if emit:
                emit("playback_pacing_stall", jitter_ms=round((now - deadline) * 1000))
            deadline = now
        if control:
            chunk = control.apply(chunk)
        await write(chunk)
        sent += len(chunk)
        if on_frame is not None:
            await on_frame(chunk)
        deadline += len(chunk) / PCM.bytes_per_second
        now = clock()
        if now > deadline:
            # A blocked write must not be followed by an immediate catch-up frame.
            if emit:
                emit("playback_pacing_stall", jitter_ms=round((now - deadline) * 1000))
            deadline = now + len(chunk) / PCM.bytes_per_second
        await sleep(max(0, deadline - now))
        return True

    async with aclosing(chunks) as stream:
        async for incoming in stream:
            if not isinstance(incoming, bytes) or len(incoming) > max_chunk_bytes:
                raise ValueError("Invalid PCM stream chunk")
            received += len(incoming)
            if received > max_bytes:
                raise ValueError("PCM stream exceeds configured limit")
            pending.extend(incoming)
            while len(pending) >= PCM.frame_bytes:
                chunk = bytes(pending[: PCM.frame_bytes])
                del pending[: PCM.frame_bytes]
                if not await deliver(chunk):
                    return PlaybackResult(sent, False)
        validate_pcm(bytes(pending))
        if pending and not await deliver(bytes(pending)):
            return PlaybackResult(sent, False)
    return PlaybackResult(sent, not (control and control.stopping))
