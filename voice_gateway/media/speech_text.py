"""Presentation-only pronunciation; canonical replies and prices stay unchanged."""

import re

_AMOUNT = r"\d+(?:[ \u00a0\u202f]\d{3})*(?:[.,]\d{1,2})?"
_BYN_AMOUNT = re.compile(
    rf"(?<![\w.,])(?:(?P<amount>{_AMOUNT})\s*BYN\b|BYN\s*(?P<prefix>{_AMOUNT})(?![\d.,]))",
    re.IGNORECASE,
)


def _form(number: int, one: str, few: str, many: str) -> str:
    if 11 <= number % 100 <= 14:
        return many
    return one if number % 10 == 1 else few if 2 <= number % 10 <= 4 else many


def pronounce_byn(text: str) -> str:
    """Expand explicit BYN amounts in Russian, preserving kopecks exactly."""

    def amount(match: re.Match[str]) -> str:
        raw = re.sub(r"[ \u00a0\u202f]", "", match["amount"] or match["prefix"])
        parts = re.split(r"[.,]", raw)
        rubles = int(parts[0])
        spoken = f"{rubles} " + _form(
            rubles, "белорусский рубль", "белорусских рубля", "белорусских рублей"
        )
        if len(parts) == 2 and (kopecks := int(parts[1].ljust(2, "0"))):
            spoken += f" {kopecks} " + _form(kopecks, "копейка", "копейки", "копеек")
        return spoken

    text = _BYN_AMOUNT.sub(amount, text)
    text = re.sub(r"\bв\s+BYN\b", "в белорусских рублях", text, flags=re.IGNORECASE)
    return re.sub(r"\bBYN\b", "белорусских рублей", text, flags=re.IGNORECASE)
