"""Optional suppression of stock opening/closing speech formulas.

Telephone openings are often covered by a PBX announcement, so repeating
"hello" from the agent sounds broken; a generic "how can I help" restated
after every answer sounds broken too. When enabled, one leading greeting
formula and one trailing generic offer are removed before synthesis;
anything else is passed through untouched. If only formulas remain, the
original text is kept so an authorized response never turns into silence.
Matching is intentionally narrow (short fixed lists, case-insensitive);
business-specific phrasing is a persona concern, not a transport rule.
"""

from __future__ import annotations

import re

_GREETING = re.compile(
    r"^\s*(здравствуйте|здравствуй|добрый день|добрый вечер|доброе утро"
    r"|приветствую|привет)[\s,!.…?]*",
    re.IGNORECASE,
)

_OFFER = re.compile(
    r"(чем (я )?могу (вам )?помочь"
    r"|могу (я |ли )?(вам )?помочь"
    r"|могу ли я (еще |ещё )?(чем-то|чем-нибудь) помочь"
    r"|что вас интересует"
    r"|есть ли (у вас )?(еще |ещё )?вопросы"
    r"|остались ли вопросы"
    r"|буду рада? помочь"
    r"|обращайтесь"
    r")[\s.!?…]*$",
    re.IGNORECASE,
)


def strip_leading_greeting(text: str) -> str:
    """Remove one leading greeting formula; never return empty."""
    if not isinstance(text, str):
        raise ValueError("Invalid speech text")
    stripped = _GREETING.sub("", text, count=1).lstrip()
    return stripped or text


def strip_trailing_offer(text: str) -> str:
    """Remove one trailing generic offer question; never return empty."""
    if not isinstance(text, str):
        raise ValueError("Invalid speech text")
    parts = re.split(r"(?<=[.!?…])\s+", text.strip())
    if len(parts) > 1 and _OFFER.fullmatch(parts[-1].strip()):
        return " ".join(parts[:-1]).strip() or text
    return text


def strip_stock_formulas(text: str) -> str:
    """Apply greeting and offer suppression in one pass."""
    return strip_trailing_offer(strip_leading_greeting(text))


class GreetingStripTTS:
    """Wraps any TTS adapter; strips stock formulas before synthesis.

    Streaming is preserved when the inner adapter supports it, otherwise a
    single synthesis is split into bounded chunks, mirroring provider
    behavior without changing audio bytes.
    """

    def __init__(self, inner) -> None:
        self._inner = inner

    async def synthesize(self, text: str) -> bytes:
        return await self._inner.synthesize(strip_stock_formulas(text))

    async def synthesize_stream(self, text: str):
        cleaned = strip_stock_formulas(text)
        stream = getattr(self._inner, "synthesize_stream", None)
        if stream is None:
            audio = await self._inner.synthesize(cleaned)
            for offset in range(0, len(audio), 65536):
                yield audio[offset : offset + 65536]
            return
        async for piece in stream(cleaned):
            yield piece

    def prewarm(self) -> None:
        prewarm = getattr(self._inner, "prewarm", None)
        if prewarm is not None:
            prewarm()

    async def close(self) -> None:
        await self._inner.close()
