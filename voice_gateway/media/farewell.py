"""Detect a spoken farewell so the call can end instead of hanging open.

The agent (or the caller) closes with a fixed class of formulas; once the
closing speech has finished playing, the media session ends gracefully instead
of waiting for the caller to hang up. Matching is deliberately narrow and
end-anchored: a long answer that merely mentions "до свидания" is not a
farewell, and a stray word never terminates an authorized response.
"""

from __future__ import annotations

import re

# Longest phrases first so the end anchor reports the most specific match.
_FAREWELLS = (
    # Russian
    "всего вам доброго", "всего доброго", "всего хорошего", "всего наилучшего",
    "всего лучшего", "всего вам хорошего", "до скорой встречи", "до скорого",
    "до встречи", "до свидания", "до свиданья", "досвидания", "дасведания",
    "до связи", "до завтра", "до вечера", "до понедельника", "хорошего дня",
    "хорошего вечера", "удачного дня", "доброго дня", "доброго вечера",
    "доброй ночи", "спасибо за звонок", "спасибо за обращение", "спасибо за визит",
    "будьте здоровы", "будьте счастливы", "рад был помочь", "рада была помочь",
    "рады были помочь", "всегда рады помочь", "рад помочь", "ждём вас снова",
    "ждем вас снова", "ждём вас", "ждем вас", "обращайтесь", "звоните нам",
    "звоните", "увидимся снова", "увидимся", "счастливо", "прощайте", "прощай",
    "пока-пока", "бай-бай", "пока", "бай", "бб",
    # English
    "have a great day", "have a nice day", "have a good day", "thanks for calling",
    "thank you for calling", "see you soon", "see you", "take care", "goodbye",
    "good bye", "bye-bye", "good night", "farewell", "bye",
    # Belarusian
    "да пабачэння", "да сустрэчы", "да спаткання", "усяго добрага",
    "усяго найлепшага", "добрага дня", "дзякуй за званок", "бывайце", "бывай",
)

MAX_FAREWELL_CHARS = 240
_NORMALIZE = re.compile(r"[^0-9a-zа-яёіў\- ]+")
_SPACES = re.compile(r"\s+")


def _normalize(text: str) -> str:
    folded = text.casefold()
    folded = _NORMALIZE.sub(" ", folded)
    return _SPACES.sub(" ", folded).strip(" -")


_OPENING_WORDS = (
    "здравствуйте", "здравствуй", "добрый день", "добрый вечер", "доброе утро",
    "приветствую", "привет",
)


def stock_opening_key(text: str) -> str | None:
    """Normalized key for a repeated stock opening, else None.

    Only greetings and the stock "вы обратились в ..." introduction qualify, so
    a legitimately repeated question is never silenced.
    """
    if not isinstance(text, str):
        raise ValueError("Invalid speech text")
    normalized = _normalize(text)
    if not normalized or len(normalized) > 200:
        return None
    if normalized.startswith(_OPENING_WORDS) or "вы обратились" in normalized:
        return normalized
    return None


def is_farewell(text: str) -> bool:
    """True when the text ends with a closing formula and stays short."""
    if not isinstance(text, str):
        raise ValueError("Invalid speech text")
    normalized = _normalize(text)
    if not normalized or len(normalized) > MAX_FAREWELL_CHARS:
        return False
    for phrase in _FAREWELLS:
        if normalized == phrase:
            return True
        if normalized.endswith(" " + phrase):
            return True
    return False
