"""Acoustic utterance assembly with conservative continuation hints.

This is a transparent heuristic, not a learned semantic end-of-turn model.
It does not schedule or own Main Chota AgentTurns.
"""

import asyncio
import logging
import time

from .observability import emit_event


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
    def __init__(
        self,
        callback,
        *,
        silence_seconds=0.5,
        continuation_seconds=0.9,
        max_pending=8,
        max_pending_chars=16000,
        emit=None,
    ):
        if max_pending < 1 or max_pending_chars < 1:
            raise ValueError("Invalid acoustic final buffer limits")
        self.callback = callback
        self.max_pending = max_pending
        self.max_pending_chars = max_pending_chars
        self.pending_chars = 0
        self.space = asyncio.Event()
        self.emit = lambda event, **values: emit_event(event, observer=emit, **values)
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

    def speech(self, *, ended_at=None):
        self.last_speech = time.monotonic() if ended_at is None else ended_at
        self.changed.set()

    async def submit(self, text, *, speech_ended_at=None):
        text = text.strip()
        if self.closed or not text:
            return
        if len(text) > self.max_pending_chars:
            raise ValueError("Acoustic final exceeds buffer character limit")
        while (
            len(self.pending) >= self.max_pending
            or self.pending_chars + len(text) + bool(self.pending) > self.max_pending_chars
        ):
            self.emit(
                "media_utterance_backpressure",
                pending_tasks=len(self.pending),
                text_chars=self.pending_chars,
            )
            self.space.clear()
            await self.space.wait()
            if self.closed:
                return
        self.pending_chars += len(text) + bool(self.pending)
        self.pending.append(text)
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
            self.pending_chars = 0
            self.space.set()
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
        self.pending_chars = 0
        self.pending_speech_at = None
        self.space.set()
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
