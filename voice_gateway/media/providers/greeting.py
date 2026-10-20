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

from ..interfaces import BaseTTS

_GREETING = re.compile(
    r"^\s*(здравствуйте|здравствуй|добрый день|добрый вечер|доброе утро"
    r"|приветствую|привет)[\s,!.…?]*",
    re.IGNORECASE,
)

# A stock self-introduction ("Вы обратились в Салон красоты ...") that the PBX
# acknowledgment already covers.
_INTRO = re.compile(
    r"^\s*(вы\s+обратились\s+в|это\s+салон|вас\s+приветствует)[^.!?…]*[.!?…]\s*",
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


def _strip_leading(text: str) -> str:
    """Drop one leading greeting and one stock self-introduction; may be empty."""
    stripped = _GREETING.sub("", text, count=1).lstrip()
    return _INTRO.sub("", stripped, count=1).lstrip()


def strip_leading_greeting(text: str) -> str:
    """Remove one leading greeting and one stock self-introduction; never empty."""
    if not isinstance(text, str):
        raise ValueError("Invalid speech text")
    return _strip_leading(text) or text


def has_meaningful_speech(text: str) -> bool:
    """False when a segment is only stock greeting/introduction formulas.

    A model that streams a call opening as separate sentences sends
    "Здравствуйте! Вы обратились в ..." first; that segment carries no content,
    so speaking it would repeat a greeting the telephone station already made.
    """
    if not isinstance(text, str):
        raise ValueError("Invalid speech text")
    return bool(_strip_leading(text))


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


class GreetingStripTTS(BaseTTS):
    """Wraps any TTS adapter; strips stock formulas before synthesis.

    Streaming is preserved when the inner adapter supports it, otherwise a
    single synthesis is split into bounded chunks, mirroring provider
    behavior without changing audio bytes.
    """

    def __init__(self, inner) -> None:
        self._inner = inner

    @property
    def capabilities(self):
        return self._inner.capabilities

    async def synthesize(self, text: str) -> bytes:
        return await self._inner.synthesize(strip_stock_formulas(text))

    async def synthesize_stream(self, text: str):
        cleaned = strip_stock_formulas(text)
        async for piece in self._inner.synthesize_stream(cleaned):
            yield piece

    def prewarm(self) -> None:
        self._inner.prewarm()

    async def close(self) -> None:
        await self._inner.close()
