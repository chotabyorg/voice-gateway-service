"""Acoustic utterance assembly with conservative continuation hints.

This is a transparent heuristic, not a learned semantic end-of-turn model.
It does not schedule or own Main Chota AgentTurns.
"""

import asyncio
import logging
import time


def likely_unfinished(text):
    text = " ".join(text.casefold().split()).rstrip(" .,!?:;…")
    tails = (
        " и",
        " но",
        " потому что",
        " чтобы",
        " если",
        " или",
        " например",
        " я хочу",
        " мне нужно",
        " я хотел",
        " подождите",
        " секунду",
        " and",
        " but",
        " because",
        " if",
        " or",
        " i need",
        " i want",
        " let me think",
        " wait a moment",
        " і",
        " але",
        " таму што",
        " калі",
        " каб",
        " мне трэба",
        " я хачу",
        " пачакайце",
    )
    return any((" " + text).endswith(tail) for tail in tails)


class UtteranceCoordinator:
    def __init__(self, callback, *, silence_seconds=0.5, continuation_seconds=0.9):
        self.callback = callback
        self.silence_seconds = silence_seconds
        self.continuation_seconds = continuation_seconds
        self.last_speech = 0.0
        self.pending = []
        self.deadline = 0.0
        self.changed = asyncio.Event()
        self.task = None
        self.closed = False
        self.pending_speech_at = None
        self.dispatched_speech_at = None

    def speech(self):
        self.last_speech = time.monotonic()
        self.changed.set()

    async def submit(self, text, *, speech_ended_at=None):
        if self.closed or not text.strip():
            return
        self.pending.append(text.strip())
        self.pending_speech_at = speech_ended_at
        self.deadline = time.monotonic() + (
            self.continuation_seconds if likely_unfinished(text) else 0
        )
        self.changed.set()
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._run(), name="caller-turn-coordinator")

    async def _run(self):
        while self.pending and not self.closed:
            self.changed.clear()
            delay = max(self.deadline, self.last_speech + self.silence_seconds) - time.monotonic()
            if delay > 0:
                try:
                    await asyncio.wait_for(self.changed.wait(), delay)
                    continue
                except TimeoutError:
                    continue
            text, self.pending = " ".join(self.pending), []
            self.dispatched_speech_at = self.pending_speech_at
            self.pending_speech_at = None
            try:
                await self.callback(text)
            except Exception as exc:
                logging.getLogger(__name__).error(
                    "voice_utterance_dispatch_failed",
                    extra={"error_type": type(exc).__name__},
                )
            finally:
                self.dispatched_speech_at = None

    async def close(self):
        self.closed = True
        self.pending.clear()
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
