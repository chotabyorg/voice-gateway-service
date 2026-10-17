"""Farewell detection and repeated-opening suppression."""

import pytest

from voice_gateway.media.farewell import is_farewell, stock_opening_key


@pytest.mark.parametrize(
    "text",
    [
        "До свидания!",
        "Всего доброго.",
        "Всего хорошего, до встречи!",
        "Хорошего дня!",
        "Удачного дня.",
        "Спасибо за звонок, до свидания!",
        "Спасибо за обращение!",
        "Пока!",
        "Пока-пока 🙂",
        "Прощайте!",
        "Увидимся.",
        "Обращайтесь!",
        "Будьте здоровы.",
        "До связи.",
        "До завтра!",
        "Bye!",
        "Goodbye, take care!",
        "See you soon.",
        "Thanks for calling!",
        "Да пабачэння!",
        "Усяго добрага.",
        "Бывайце.",
    ],
)
def test_farewell_variants_are_detected(text):
    assert is_farewell(text) is True


@pytest.mark.parametrize(
    "text",
    [
        "Сколько стоит маникюр?",
        "До свидания, но сначала уточните время записи",
        "Пока что не готово",
        "Нажмите кнопку, пока не появится меню",
        "Мы работаем с девяти до девяти.",
        "",
        "пока" + " очень длинное продолжение" * 20,
    ],
)
def test_non_farewell_text_is_not_a_farewell(text):
    assert is_farewell(text) is False


def test_stock_opening_covers_greeting_and_intro_only():
    key = stock_opening_key("Здравствуйте! Вы обратились в Салон красоты «У Светланы».")
    assert key is not None
    assert stock_opening_key(key) == key
    assert stock_opening_key("Вы обратились в салон красоты.") is not None
    assert stock_opening_key("Сколько стоит маникюр?") is None
    assert stock_opening_key("Конечно. Подскажите, на какой день?") is None


def test_farewell_rejects_non_text():
    with pytest.raises(ValueError):
        is_farewell(None)
