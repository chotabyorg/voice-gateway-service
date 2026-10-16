"""Optional suppression of a leading spoken greeting.

Telephone openings are often covered by a PBX announcement, so repeating
"hello" from the agent sounds broken. When enabled, the first greeting
formula is removed before synthesis; anything else is passed through
untouched. If only a greeting remains, the original text is kept so an
authorized response never turns into silence. Matching is intentionally
narrow (a short fixed list, case-insensitive); business-specific
introductions are a persona concern, not a transport rule.
"""

from __future__ import annotations

import re

_GREETING = re.compile(
    r"^\s*(здравствуйте|здравствуй|добрый день|добрый вечер|доброе утро"
    r"|приветствую|привет)[\s,!.…?]*",
    re.IGNORECASE,
)


def strip_leading_greeting(text: str) -> str:
    """Remove one leading greeting formula; never return empty."""
    if not isinstance(text, str):
        raise ValueError("Invalid speech text")
    stripped = _GREETING.sub("", text, count=1).lstrip()
    return stripped or text


class GreetingStripTTS:
    """Wraps any TTS adapter; strips the greeting before synthesis.

    Streaming is preserved when the inner adapter supports it, otherwise a
    single synthesis is split into bounded chunks, mirroring provider
    behavior without changing audio bytes.
    """

    def __init__(self, inner) -> None:
        self._inner = inner

    async def synthesize(self, text: str) -> bytes:
        return await self._inner.synthesize(strip_leading_greeting(text))

    async def synthesize_stream(self, text: str):
        cleaned = strip_leading_greeting(text)
        stream = getattr(self._inner, "synthesize_stream", None)
        if stream is None:
            audio = await self._inner.synthesize(cleaned)
            for offset in range(0, len(audio), 65536):
                yield audio[offset : offset + 65536]
            return
        async for piece in stream(cleaned):
            yield piece

    async def close(self) -> None:
        await self._inner.close()
