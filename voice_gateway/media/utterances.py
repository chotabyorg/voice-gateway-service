"""Acoustic utterance assembly with conservative continuation hints.

This is a transparent heuristic, not a learned semantic end-of-turn model.
It does not schedule or own Main Chota AgentTurns.
"""

import asyncio
import logging
import re
import time

from .observability import emit_event
from .phone_endpoint import (
    complete_phone_dictation,
    complete_phone_suffix,
    phone_dictation_fragment,
)


_NUMBER_WORDS = frozenset(
    """
ноль один два три четыре пять шесть семь восемь девять десять одиннадцать
двенадцать тринадцать четырнадцать пятнадцать шестнадцать семнадцать
восемнадцать девятнадцать двадцать тридцать сорок пятьдесят шестьдесят
семьдесят восемьдесят девяносто сто двести триста четыреста пятьсот шестьсот
семьсот восемьсот девятьсот тысяча плюс минус код номер телефон добавочный
zero one two three four five six seven eight nine ten plus minus code number
""".split()
)
_NON_NUMERIC = re.compile(r"[^0-9a-zа-яёіў]+")


def numeric_dictation(text):
    """True when the final looks like a dictated phone number or its fragment.

    VAD/STT cut a spoken number into short, digit-heavy finals; those must not be
    answered as a finished request.
    """
    if not text or len(text) > 80:
        return False
    tokens = [token for token in _NON_NUMERIC.split(text.casefold()) if token]
    if not tokens or len(tokens) > 12:
        return False
    numeric = sum(1 for token in tokens if token.isdigit() or token in _NUMBER_WORDS)
    return numeric >= 1 and numeric * 4 >= len(tokens) * 3


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
    return any((" " + text).endswith(tail) for tail in tails) or numeric_dictation(text)


class UtteranceCoordinator:
    def __init__(
        self,
        callback,
        *,
        silence_seconds=0.5,
        continuation_seconds=0.9,
        numeric_continuation_seconds=2.5,
        max_pending=8,
        max_pending_chars=16000,
        emit=None,
    ):
        if max_pending < 1 or max_pending_chars < 1:
            raise ValueError("Invalid acoustic final buffer limits")
        if numeric_continuation_seconds < continuation_seconds:
            raise ValueError("Numeric continuation must not shorten the window")
        self.callback = callback
        self.max_pending = max_pending
        self.max_pending_chars = max_pending_chars
        self.pending_chars = 0
        self.space = asyncio.Event()
        self.emit = lambda event, **values: emit_event(event, observer=emit, **values)
        self.silence_seconds = silence_seconds
        self.continuation_seconds = continuation_seconds
        self.numeric_continuation_seconds = numeric_continuation_seconds
        self.last_speech = 0.0
        self.pending = []
        self.deadline = 0.0
        self.changed = asyncio.Event()
        self.task = None
        self.closed = False
        self.pending_speech_at = None
        self.dispatched_speech_at = None
        self.short_phone_speech_at = None

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
        combined = " ".join(self.pending)
        if complete_phone_dictation(combined) or complete_phone_suffix(combined):
            window = self.continuation_seconds
            self.short_phone_speech_at = (
                self.last_speech if speech_ended_at is None else speech_ended_at
            )
        elif numeric_dictation(text) or phone_dictation_fragment(combined):
            window = self.numeric_continuation_seconds
            self.short_phone_speech_at = None
        elif likely_unfinished(text):
            window = self.continuation_seconds
            self.short_phone_speech_at = None
        else:
            # A quick provider final can precede the caller's next acoustic
            # fragment. Give observed continuations a short chance to arrive;
            # adapters without coverage metadata retain their existing policy.
            window = min(self.silence_seconds, 0.3) if speech_ended_at is not None else 0
            self.short_phone_speech_at = None
        self.deadline = time.monotonic() + window
        self.changed.set()
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._run(), name="caller-turn-coordinator")

    async def _run(self):
        while self.pending and not self.closed:
            self.changed.clear()
            deadline = max(self.deadline, self.last_speech + self.silence_seconds)
            covered_speech_at = self.pending_speech_at
            if covered_speech_at is None:
                # Preserve the existing phone heuristic for adapters without
                # an explicit acoustic timestamp on their final transcript.
                covered_speech_at = self.short_phone_speech_at
            if covered_speech_at is not None and self.last_speech > covered_speech_at:
                # Newer classified speech can still be awaiting recognition,
                # including a contact dictated after a finished request. Silence
                # alone does not prove that its final transcript has arrived.
                # A missing final releases the input at the same bounded window.
                deadline = max(deadline, self.last_speech + self.numeric_continuation_seconds)
            delay = deadline - time.monotonic()
            if delay > 0:
                try:
                    await asyncio.wait_for(self.changed.wait(), delay)
                    continue
                except TimeoutError:
                    continue
            text, self.pending = " ".join(self.pending), []
            self.pending_chars = 0
            self.short_phone_speech_at = None
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
        self.short_phone_speech_at = None
        self.space.set()
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
