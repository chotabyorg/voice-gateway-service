"""Early provider work never becomes a final until acoustic endpoint confirmation."""

import asyncio

import pytest

from tests.media.media_fakes import pcm
from voice_gateway.media.batch_stt import BatchSTT, BatchSTTSettings
from voice_gateway.media.vad import VADSettings


class ProbeSTT(BatchSTT):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.requests = []
        self.entered = asyncio.Queue()
        self.release = asyncio.Event()
        self.finals = []

    async def _transcribe(self, audio):
        self.requests.append(audio)
        index = len(self.requests)
        self.entered.put_nowait(index)
        await self.release.wait()
        return f"fixture {index}"

    async def final(self, text):
        self.finals.append((text, self.final_speech_at))


async def started(**settings):
    stt = ProbeSTT(queue=BatchSTTSettings(early_start_ms=200, **settings))
    await stt.start_stream(on_final=stt.final)
    return stt


async def test_request_overlaps_endpoint_wait_but_final_cannot_escape_early():
    stt = await started(max_pending=1)
    try:
        await stt.feed_audio(pcm(1200, 100) + pcm(0, 200))
        speech_at = stt.last_speech_at
        assert await asyncio.wait_for(stt.entered.get(), 1) == 1
        stt.release.set()
        await asyncio.sleep(0)
        await stt.feed_audio(pcm(0, 280))
        assert stt.finals == [] and len(stt._active_tasks) == 1
        await stt.feed_audio(pcm(0, 20))
        await stt.stop_stream()
        assert stt.finals == [("fixture 1", speech_at)]
        assert stt.requests == [pcm(1200, 100) + pcm(0, 200)]
        assert not stt._active_tasks and not stt.vad.buffer
    finally:
        await stt.abort_stream()


async def test_resumed_speech_discards_early_text_and_recognizes_complete_audio_once():
    stt = await started()
    first = pcm(1200, 100) + pcm(0, 200)
    rest = pcm(1600, 100) + pcm(0, 240) + pcm(1600, 100) + pcm(0, 500)
    try:
        stt.release.set()
        await stt.feed_audio(first)
        await asyncio.wait_for(stt.entered.get(), 1)
        await asyncio.sleep(0)
        for start in range(0, len(rest), 320):
            await stt.feed_audio(rest[start : start + 320])
            await asyncio.sleep(0)
        await stt.stop_stream()
        assert stt.requests == [first, first + rest]
        assert [text for text, _ in stt.finals] == ["fixture 2"]
    finally:
        await stt.abort_stream()


async def test_unconfirmed_request_respects_concurrency_and_backlog():
    stt = await started(concurrency=1, max_pending=1)
    try:
        await stt.feed_audio(pcm(1200, 100) + pcm(0, 500))
        await asyncio.wait_for(stt.entered.get(), 1)
        await stt.feed_audio(pcm(1200, 100) + pcm(0, 200))
        assert len(stt.requests) == len(stt._active_tasks) == 1
        assert stt._early_task is None
        with pytest.raises(RuntimeError, match="backlog"):
            await stt.feed_audio(pcm(0, 300))
        assert stt.vad.buffer_bytes == len(pcm(1200, 100) + pcm(0, 500))
    finally:
        await stt.abort_stream()
    assert stt.finals == [] and not stt._active_tasks


async def test_early_fast_second_result_cannot_overtake_first():
    class Ordered(ProbeSTT):
        async def _transcribe(self, audio):
            self.requests.append(audio)
            index = len(self.requests)
            self.entered.put_nowait(index)
            if index == 1:
                await self.release.wait()
            return f"fixture {index}"

    stt = Ordered(queue=BatchSTTSettings(early_start_ms=200))
    await stt.start_stream(on_final=stt.final)
    try:
        for _ in range(2):
            await stt.feed_audio(pcm(1200, 100) + pcm(0, 500))
            await asyncio.wait_for(stt.entered.get(), 1)
        await asyncio.sleep(0)
        assert stt.finals == []
        stt.release.set()
        await stt.stop_stream()
        assert [text for text, _ in stt.finals] == ["fixture 1", "fixture 2"]
    finally:
        await stt.abort_stream()


async def test_hangup_fences_cancellation_resistant_early_provider_and_restart():
    class Resistant(ProbeSTT):
        async def _transcribe(self, audio):
            self.entered.put_nowait(True)
            while not self.release.is_set():
                try:
                    await self.release.wait()
                except asyncio.CancelledError:
                    pass
            return "late fixture"

    stt = Resistant(queue=BatchSTTSettings(early_start_ms=200, shutdown_timeout=0.04))
    await stt.start_stream(on_final=stt.final)
    await stt.feed_audio(pcm(1200, 100) + pcm(0, 200))
    await asyncio.wait_for(stt.entered.get(), 1)
    tasks = tuple(stt._active_tasks)
    try:
        await asyncio.wait_for(stt.abort_stream(), 0.3)
        with pytest.raises(RuntimeError, match="pending"):
            await stt.start_stream()
    finally:
        stt.release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert stt.finals == [] and not stt._active_tasks and not stt.vad.buffer
    await stt.start_stream(on_final=stt.final)
    await stt.abort_stream()


@pytest.mark.parametrize("result", ["", RuntimeError("provider fixture")])
async def test_empty_or_failed_early_result_does_not_retry_or_deliver(result):
    class Failed(ProbeSTT):
        async def _transcribe(self, audio):
            self.requests.append(audio)
            if isinstance(result, Exception):
                raise result
            return result

    stt = Failed(queue=BatchSTTSettings(early_start_ms=200))
    await stt.start_stream(on_final=stt.final)
    await stt.feed_audio(pcm(1200, 100) + pcm(0, 200))
    await asyncio.sleep(0)
    await stt.feed_audio(pcm(0, 300))
    await stt.stop_stream()
    assert len(stt.requests) == 1 and stt.finals == []


async def test_stop_can_confirm_existing_request_even_at_backlog_capacity():
    stt = await started(max_pending=1)
    stt.release.set()
    await stt.feed_audio(pcm(1200, 100) + pcm(0, 200))
    await asyncio.wait_for(stt.entered.get(), 1)
    await stt.stop_stream()
    assert len(stt.requests) == len(stt.finals) == 1 and not stt._active_tasks


async def test_resumed_speech_fences_late_provider_that_suppresses_cancellation():
    class ResistantFirst(ProbeSTT):
        async def _transcribe(self, audio):
            self.requests.append(audio)
            index = len(self.requests)
            self.entered.put_nowait(index)
            if index == 1:
                while not self.release.is_set():
                    try:
                        await self.release.wait()
                    except asyncio.CancelledError:
                        pass
            return f"fixture {index}"

    stt = ResistantFirst(queue=BatchSTTSettings(early_start_ms=200))
    await stt.start_stream(on_final=stt.final)
    try:
        await stt.feed_audio(pcm(1200, 100) + pcm(0, 200))
        await asyncio.wait_for(stt.entered.get(), 1)
        await stt.feed_audio(pcm(1200, 100) + pcm(0, 500))
        await asyncio.wait_for(stt.entered.get(), 1)
        await asyncio.sleep(0)
        assert [text for text, _ in stt.finals] == ["fixture 2"]
    finally:
        stt.release.set()
        await stt.stop_stream()
    assert [text for text, _ in stt.finals] == ["fixture 2"]


@pytest.mark.parametrize("packet_bytes", [2, 126, 320, 962, 4096])
async def test_early_snapshot_is_packet_independent_and_clicks_do_not_start_requests(packet_bytes):
    stt = await started()
    stt.release.set()
    audio = pcm(1200, 20) + pcm(0, 500) + pcm(1200, 100) + pcm(0, 500)
    for start in range(0, len(audio), packet_bytes):
        await stt.feed_audio(audio[start : start + packet_bytes])
        await asyncio.sleep(0)
    await stt.stop_stream()
    assert len(stt.requests) == 1
    assert stt.requests[0].endswith(pcm(1200, 100) + pcm(0, 200))
    assert [text for text, _ in stt.finals] == ["fixture 1"]


async def test_disabled_by_default_and_invalid_early_endpoint_rejected():
    stt = ProbeSTT()
    await stt.start_stream()
    await stt.feed_audio(pcm(1200, 100) + pcm(0, 200))
    await asyncio.sleep(0)
    assert not stt.requests
    await stt.abort_stream()
    with pytest.raises(ValueError):
        BatchSTTSettings(early_start_ms=-1)
    with pytest.raises(ValueError, match="precede"):
        BatchSTT(vad=VADSettings(silence_end_ms=200), queue=BatchSTTSettings(early_start_ms=200))
