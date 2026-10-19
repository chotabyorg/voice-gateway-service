"""Conservative phone-shaped acoustic finals; never a contact or identity parser.

The result controls only the continuation wait. Text is never rewritten, and
this module cannot validate a CRM contact or grant a canonical turn.
"""

import re
import unicodedata

_PLUS = re.compile(r"\+|\bплюс\b")
_UNCERTAIN = re.compile(
    r"\b(?:не|нет|или|либо|вернее|точнее|ошиб\w*|исправ\w*|поправ\w*|последн\w*|добавоч\w*)\b"
)
_DIGITAL_BY_PHONE = re.compile(
    r"\+375(?:[ -]?[0-9]{2}| ?\([0-9]{2}\))[ -]?[0-9]{3}[ -]?[0-9]{2}[ -]?[0-9]{2}"
)
_UNITS = dict(zip("ноль один два три четыре пять шесть семь восемь девять".split(), range(10)))
_UNITS.update(нуль=0, одна=1, две=2)
_TEENS = dict(
    zip(
        "десять одиннадцать двенадцать тринадцать четырнадцать пятнадцать шестнадцать "
        "семнадцать восемнадцать девятнадцать".split(),
        range(10, 20),
    )
)
_TENS = dict(
    zip(
        "двадцать тридцать сорок пятьдесят шестьдесят семьдесят восемьдесят девяносто".split(),
        range(20, 100, 10),
    )
)
_HUNDREDS = dict(
    zip(
        "сто двести триста четыреста пятьсот шестьсот семьсот восемьсот девятьсот".split(),
        range(100, 1000, 100),
    )
)
_NUMBER_WORDS = _UNITS.keys() | _TEENS.keys() | _TENS.keys() | _HUNDREDS.keys()


def _spoken_groups(tokens):
    groups = []
    position = 0
    while position < len(tokens):
        token = tokens[position]
        if token.isascii() and token.isdecimal():
            groups.append(token)
            position += 1
            continue
        value = 0
        consumed = False
        if token in _HUNDREDS:
            value = _HUNDREDS[token]
            position += 1
            consumed = True
        if position < len(tokens) and tokens[position] in _TEENS:
            value += _TEENS[tokens[position]]
            position += 1
            consumed = True
        else:
            if position < len(tokens) and tokens[position] in _TENS:
                value += _TENS[tokens[position]]
                position += 1
                consumed = True
            if position < len(tokens) and tokens[position] in _UNITS:
                # A spoken zero starts its own group; do not lose leading zeros.
                if _UNITS[tokens[position]] != 0 or not consumed:
                    value += _UNITS[tokens[position]]
                    position += 1
                    consumed = True
        if not consumed:
            return None
        groups.append(str(value))
    return "".join(groups)


def complete_phone_dictation(text):
    """Shorten only one explicit, structurally complete +375 acoustic final.

    Bounded leading name/contact words and Russian number groups are supported.
    Alternatives, corrections, extensions and unsupported formats retain the
    conservative wait. This is an acoustic hint rather than phone validation.
    """
    if not isinstance(text, str) or len(text) > 256:
        return False
    text = unicodedata.normalize("NFC", " ".join(text.casefold().split())).rstrip(".!?")
    if _UNCERTAIN.search(text):
        return False
    markers = list(_PLUS.finditer(text))
    if len(markers) != 1:
        return False
    marker = markers[0]
    prefix = text[: marker.start()]
    prefix_words = re.findall(r"[а-яёіўa-z]+", prefix)
    if (
        len(prefix) > 80
        or len(prefix_words) > 8
        or not re.fullmatch(r"[а-яёіўa-z\s,.:;!?-]*", prefix)
        or any(word in _NUMBER_WORDS for word in prefix_words)
    ):
        return False
    body = text[marker.end() :].strip()
    if _DIGITAL_BY_PHONE.fullmatch("+" + body):
        return True
    if not re.fullmatch(r"[а-яё0-9\s,]+", body):
        return False
    tokens = body.replace(",", " ").split()
    if len(tokens) > 24 or not any(word in _NUMBER_WORDS for word in tokens):
        return False
    digits = _spoken_groups(tokens)
    return bool(digits is not None and re.fullmatch(r"375[0-9]{9}", digits))


def phone_dictation_fragment(text):
    """Keep explicit fragments on the long wait within the caller's buffer bound."""
    if not isinstance(text, str):
        return False
    marker = _PLUS.search(text.casefold())
    if marker is None:
        return False
    tail = text[marker.end() :].casefold()
    return bool(re.search(r"[0-9]|\b(?:" + "|".join(_NUMBER_WORDS) + r")\b", tail))


def complete_phone_suffix(text):
    """Use the existing short hold when a bounded request ends in one full phone.

    A preceding request may contain dates or times, unlike the short name/contact
    prefix accepted by complete_phone_dictation. The same strict phone parser and
    whole-input correction/ambiguity checks still apply. Nothing is rewritten or
    treated as a validated contact, business intent or execution permission.
    """
    if not isinstance(text, str) or len(text) > 512:
        return False
    text = unicodedata.normalize("NFC", " ".join(text.casefold().split())).rstrip(".!?")
    if _UNCERTAIN.search(text):
        return False
    markers = list(_PLUS.finditer(text))
    if len(markers) != 1:
        return False
    marker = markers[0]
    if not re.fullmatch(r"[а-яёіўa-z0-9\s,.:;!?-]*", text[: marker.start()]):
        return False
    return complete_phone_dictation(text[marker.start() :])
