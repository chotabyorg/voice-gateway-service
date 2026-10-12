"""Paced PCM delivery. No recording, dialogue, tenant or analytics policy."""

import asyncio
import time
from dataclasses import dataclass
from .audio import PCM, validate_pcm


@dataclass(frozen=True)
class PlaybackResult:
    sent_bytes: int
    complete: bool


async def play_pcm(
    audio,
    write,
    *,
    on_frame=None,
    should_continue=lambda: True,
    clock=time.monotonic,
    sleep=asyncio.sleep,
):
    validate_pcm(audio)
    deadline, sent = clock(), 0
    for offset in range(0, len(audio), PCM.frame_bytes):
        if not should_continue():
            return PlaybackResult(sent, False)
        chunk = audio[offset : offset + PCM.frame_bytes]
        await write(chunk)
        sent += len(chunk)
        if on_frame is not None:
            await on_frame(chunk)
        deadline += len(chunk) / PCM.bytes_per_second
        now = clock()
        await sleep(max(0, deadline - now))
        if now > deadline + PCM.frame_ms / 1000:
            deadline = clock()  # Avoid bursts after a slow socket or scheduler stall.
    return PlaybackResult(sent, True)
