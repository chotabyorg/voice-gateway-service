"""Late streamed sentences cannot restart an answer the caller interrupted."""

import asyncio
import time

import pytest

from tests.media.media_fakes import FakeConversationPort, FakeTTS, pcm
from tests.media.test_media_lifecycle import start_call, until
from voice_gateway.events import AuthorizedSpeech, PlaybackState
from voice_gateway.media.call import MediaSettings


class ResponsePort(FakeConversationPort):
    def __init__(self):
        super().__init__(None)
        self.queue = asyncio.Queue()

    async def responses(self, call_id):
        while True:
            yield await self.queue.get()


@pytest.mark.parametrize("classified", [True, False])
async def test_caller_speech_in_gap_after_completed_partial_fences_its_late_final(classified):
    port = ResponsePort()
    call, task, transport, stt, tts, _ = await start_call(
        port=port, settings=MediaSettings(post_playback_suppression_seconds=0)
    )
    try:
        await port.queue.put(AuthorizedSpeech("partial", "Сейчас проверю.", True, "open"))
        await until(lambda: any(f.state == PlaybackState.COMPLETED for f in port.playback))
        await until(lambda: call._output is None)
        assert call._open_response_group == "open"
        if classified:
            stt.uses_speech_classifier = True
            stt.last_speech_at = None
        await call.feed_audio(pcm(0, 100))
        assert call._interrupted_response_group is None
        if classified:
            feed = stt.feed_audio

            async def classified_speech(audio):
                await feed(audio)
                stt.last_speech_at = time.monotonic()

            stt.feed_audio = classified_speech
        await call.feed_audio(pcm(3000, 100))
        assert call._interrupted_response_group == "open"
        await port.queue.put(AuthorizedSpeech("late", "Старое продолжение.", False, "open"))
        await until(lambda: any(f.request_id == "late" for f in port.playback))
        assert tts.texts == ["Сейчас проверю."]
        late = next(f for f in port.playback if f.request_id == "late")
        assert late.state == PlaybackState.NOT_STARTED
        assert late.generated_bytes == late.transport_accepted_bytes == 0
        assert call._open_response_group is None
    finally:
        await transport.incoming.put(None)
        await asyncio.wait_for(task, 1)


@pytest.mark.parametrize("suffix_queued_before_barge_in", [True, False])
async def test_barge_in_fences_queued_and_late_suffixes_but_allows_new_reply(
    suffix_queued_before_barge_in,
):
    port = ResponsePort()
    call, task, transport, stt, tts, _ = await start_call(port=port, tts=FakeTTS(pcm(2000, 1000)))
    suffix = AuthorizedSpeech("suffix", "Выберите время записи.", response_group="old")
    try:
        await port.queue.put(
            AuthorizedSpeech("partial", "Проверяю время записи.", True, response_group="old")
        )
        await asyncio.wait_for(transport.wrote.wait(), 1)
        if suffix_queued_before_barge_in:
            await port.queue.put(suffix)
        # Exercise the real acoustic interruption path, before an STT final.
        for _ in range(5):
            await call.feed_audio(pcm(3000))
        if not suffix_queued_before_barge_in:
            await port.queue.put(suffix)
        await until(lambda: any(f.request_id == "suffix" for f in port.playback))
        facts = [f for f in port.playback if f.request_id == "suffix"]
        assert len(facts) == 1
        assert facts[0].state == PlaybackState.NOT_STARTED
        assert facts[0].generated_bytes == facts[0].transport_accepted_bytes == 0
        assert tts.texts == ["Проверяю время записи."]
        assert any(f.state == PlaybackState.INTERRUPTED for f in port.playback)
        assert stt.chunks  # caller's audio still reaches STT
        # A later caller turn and all its ordinary partial/final sentences work.
        tts.audio = pcm(2000, 40)
        await port.queue.put(AuthorizedSpeech("new-partial", "Уточняю время.", True, "new"))
        await port.queue.put(AuthorizedSpeech("new-final", "Подойдёт три часа?", False, "new"))
        await until(
            lambda: any(
                f.request_id == "new-final" and f.state == PlaybackState.COMPLETED
                for f in port.playback
            )
        )
        assert tts.texts == ["Проверяю время записи.", "Уточняю время.", "Подойдёт три часа?"]
        assert call._awaiting_since is None
    finally:
        await transport.incoming.put(None)
        await asyncio.wait_for(task, 1)


async def test_late_suffix_does_not_clear_pending_caller_answer_or_leak_to_another_call():
    port = ResponsePort()
    call, task, transport, _, tts, _ = await start_call(port=port)
    try:
        # State left by a previously interrupted reply; no new PCM may be sent.
        call._interrupted_response_group = "old"
        call._awaiting_since = 123.0
        await port.queue.put(AuthorizedSpeech("late", "Старый ответ.", response_group="old"))
        await until(lambda: bool(port.playback))
        assert call._awaiting_since == 123.0
        assert not tts.texts and not transport.sent

        other, other_task, other_transport, _, other_tts, other_port = await start_call()
        try:
            assert await other.speak("independent", "Новый ответ.", response_group="old")
            assert other_tts.texts == ["Новый ответ."]
            assert other_port.playback[-1].state == PlaybackState.COMPLETED
        finally:
            await other_transport.incoming.put(None)
            await asyncio.wait_for(other_task, 1)
    finally:
        await transport.incoming.put(None)
        await asyncio.wait_for(task, 1)


async def test_three_consecutive_interruptions_allow_only_the_next_response_group():
    port = ResponsePort()
    call, task, transport, stt, tts, _ = await start_call(
        port=port,
        tts=FakeTTS(pcm(2000, 1000)),
        settings=MediaSettings(post_playback_suppression_seconds=0),
    )
    try:
        for index in range(3):
            group = f"answer-{index}"
            transport.wrote.clear()
            await port.queue.put(AuthorizedSpeech(group, "Проверяю время.", True, group))
            await asyncio.wait_for(transport.wrote.wait(), 1)
            for _ in range(5):
                await call.feed_audio(pcm(3000))
            await until(
                lambda: any(
                    f.request_id == group and f.state == PlaybackState.INTERRUPTED
                    for f in port.playback
                )
            )
            suffix = f"suffix-{index}"
            await port.queue.put(AuthorizedSpeech(suffix, "Старое продолжение.", False, group))
            await until(lambda: any(f.request_id == suffix for f in port.playback))
            rejected = [f for f in port.playback if f.request_id == suffix]
            assert len(rejected) == 1 and rejected[0].state == PlaybackState.NOT_STARTED
            assert rejected[0].generated_bytes == rejected[0].transport_accepted_bytes == 0
        assert len(stt.chunks) >= 15
        assert tts.texts == ["Проверяю время."] * 3
        tts.audio = pcm(2000, 40)
        await port.queue.put(AuthorizedSpeech("fresh", "Уточните время.", False, "fresh"))
        await until(
            lambda: any(
                f.request_id == "fresh" and f.state == PlaybackState.COMPLETED
                for f in port.playback
            )
        )
        assert tts.texts[-1] == "Уточните время."
    finally:
        await transport.incoming.put(None)
        await asyncio.wait_for(task, 1)
