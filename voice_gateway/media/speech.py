"""Overlap language generation, one-segment speech preparation and paced playback."""

import asyncio
from contextlib import aclosing


async def play_speech_stream(
    segments, *, synthesize, play, guard, on_spoken, prefetch=True, on_start=None
):
    text_queue = asyncio.Queue(maxsize=8)

    async def produce():
        try:
            async with aclosing(segments) as stream:
                async for text in stream:
                    if text.strip():
                        await text_queue.put(("text", text.strip()))
            await text_queue.put(("end", None))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await text_queue.put(("error", exc))

    async def prepare():
        kind, value = await text_queue.get()
        if kind == "error":
            raise value
        if kind == "end":
            return None
        text, terminal = guard(value)
        return text, await synthesize(text), terminal

    producer = asyncio.create_task(produce(), name="speech-text-producer")
    pending = None
    spoken = []
    try:
        current = await prepare()
        while current is not None:
            text, audio, terminal = current
            # Only one TTS request at a time; its next result overlaps current playback.
            if prefetch and not terminal:
                pending = asyncio.create_task(prepare(), name="speech-audio-prefetch")
            if on_start is not None:
                on_start(text)
            await play(audio)
            spoken.append(text)
            on_spoken(text)
            if terminal:
                break
            current = await pending if pending is not None else await prepare()
            pending = None
        return " ".join(spoken)
    finally:
        tasks = [producer] + ([pending] if pending is not None else [])
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
