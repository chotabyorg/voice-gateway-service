"""Late streamed sentences cannot restart an answer the caller interrupted."""

import asyncio

import pytest

from tests.media.media_fakes import FakeConversationPort, FakeTTS, pcm
from tests.media.test_media_lifecycle import start_call, until
from voice_gateway.events import AuthorizedSpeech, PlaybackState


class ResponsePort(FakeConversationPort):
    def __init__(self):
        super().__init__(None)
        self.queue = asyncio.Queue()

    async def responses(self, call_id):
        while True:
            yield await self.queue.get()


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
