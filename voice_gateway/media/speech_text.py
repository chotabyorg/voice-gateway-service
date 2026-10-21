"""Presentation-only pronunciation; canonical replies and prices stay unchanged."""

import re
from datetime import date


def prepare_tts_text(text: str, *, booking_values=False, expand_byn=False) -> str:
    """One pronunciation pass on the provider's speech copy, never canonical text."""
    if not isinstance(text, str):
        raise ValueError("Invalid TTS text")
    if booking_values:
        text = pronounce_booking_values(text)
    return pronounce_byn(text) if expand_byn else text


_AMOUNT = r"\d+(?:[ \u00a0\u202f]\d{3})*(?:[.,]\d{1,2})?"
_BYN_AMOUNT = re.compile(
    rf"(?<![\w.,])(?:(?P<amount>{_AMOUNT})\s*BYN\b|BYN\s*(?P<prefix>{_AMOUNT})(?![\d.,]))",
    re.IGNORECASE,
)
_PHONE = re.compile(r"(?<![\w+])\+[1-9][0-9]{7,14}(?![\w+])")
_DATE = re.compile(
    r"(?<![\w.])(?P<day>[0-9]{1,2})\.(?P<month>[0-9]{2})"
    r"(?:\.(?P<year>[0-9]{4})(?!\w|\.[0-9])|(?=\s+в\s+[0-9]{1,2}:[0-9]{2}\b))",
    re.IGNORECASE,
)
_TIME = re.compile(
    r"(?P<prefix>\bв\s+)(?P<hour>[0-9]{1,2}):(?P<minute>[0-9]{2})(?![\w:])", re.IGNORECASE
)
_MINSK_ZONE = re.compile(r"\(Europe/Minsk\)|(?<![\w/])Europe/Minsk(?![\w/])")
_PRICE_PREFIX = re.compile(
    r"(?:\b(?:стоимость|цена|сумма|итого|оплата|стоит|ценой)\b[^0-9,.;:!?]{0,40}"
    r"[:=—–-]?\s*|\b(?:BYN|USD|EUR|RUB)\s*)$",
    re.IGNORECASE,
)
_DIGITS = ("ноль", "один", "два", "три", "четыре", "пять", "шесть", "семь", "восемь", "девять")
_MONTHS = (
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
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


def pronounce_booking_values(text: str) -> str:
    """Render explicit booking formats for Russian TTS without changing their values.

    Unmarked numbers, invalid dates, other zones and decimal prices are left alone.
    Dates without a year require the runtime's adjacent ``в HH:MM`` clock context.
    This is stateless per authorized speech chunk; canonical text is never rewritten.
    """

    def calendar_date(match: re.Match[str]) -> str:
        day, month = int(match["day"]), int(match["month"])
        year = match["year"]
        if year is None and _PRICE_PREFIX.search(text[max(0, match.start() - 64) : match.start()]):
            return match[0]
        try:
            date(int(year) if year else 2000, month, day)
        except ValueError:
            return match[0]
        return f"{day} {_MONTHS[month - 1]}" + (f" {year} года" if year else "")

    def clock(match: re.Match[str]) -> str:
        hour, minute = int(match["hour"]), int(match["minute"])
        if hour > 23 or minute > 59:
            return match[0]
        spoken = f"{hour} " + _form(hour, "час", "часа", "часов")
        if minute:
            spoken += f" {minute} " + _form(minute, "минуту", "минуты", "минут")
        return match["prefix"] + spoken

    text = _PHONE.sub(lambda match: "плюс " + " ".join(_DIGITS[int(d)] for d in match[0][1:]), text)
    text = _DATE.sub(calendar_date, text)
    text = _TIME.sub(clock, text)
    return _MINSK_ZONE.sub("по минскому времени", text)
