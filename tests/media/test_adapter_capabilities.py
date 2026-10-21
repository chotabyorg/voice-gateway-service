"""Reject incompatible media ports before acquiring call resources."""

from dataclasses import replace

import pytest

from tests.media.media_fakes import FakeConversationPort, FakeSTT, FakeTTS, FakeTransport
from voice_gateway.media.call import MediaCall
from voice_gateway.media.audio import PCM
from voice_gateway.media.providers.greeting import GreetingStripTTS
from voice_gateway.media.providers.openrouter import OpenRouterTTS
from voice_gateway.media.providers.speechkit import SpeechKitSTT, SpeechKitTTS


@pytest.mark.parametrize(
    "port,change",
    [
        ("stt", {"pcm": replace(PCM, sample_rate=16000)}),
        ("tts", {"pcm": replace(PCM, sample_rate=24000)}),
        ("tts", {"pcm": replace(PCM, channels=2)}),
        ("stt", {"final_results": False}),
        ("tts", {"max_output_chunk_bytes": 3}),
        ("stt", {"cancellation": False}),
        ("tts", {"cancellation": False}),
    ],
)
def test_incompatible_media_configuration_fails_before_call_start(port, change):
    stt, tts = FakeSTT(), FakeTTS()
    selected = stt if port == "stt" else tts
    selected.capabilities = replace(selected.capabilities, **change)
    with pytest.raises(ValueError, match="cancellable PCM16"):
        MediaCall(FakeTransport(), stt, tts, FakeConversationPort(None))
    assert not stt.chunks and not tts.texts


def test_native_streaming_and_partial_results_are_declared_separately():
    assert SpeechKitSTT.capabilities.streaming_transcription
    assert not SpeechKitSTT.capabilities.partial_results
    assert SpeechKitTTS.capabilities.streaming_synthesis
    assert OpenRouterTTS.capabilities.streaming_synthesis
    assert not FakeTTS.capabilities.streaming_synthesis
    inner = FakeTTS()
    assert GreetingStripTTS(inner).capabilities is inner.capabilities
