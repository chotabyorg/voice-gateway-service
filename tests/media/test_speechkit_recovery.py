"""Real loopback gRPC failures must not duplicate an utterance or end a recovered call."""

import asyncio
import json
import logging

import grpc
import pytest
from yandex.cloud.ai.stt.v3 import stt_pb2 as p
from yandex.cloud.ai.stt.v3 import stt_service_pb2_grpc as service

from tests.media.media_fakes import FakeConversationPort, FakeTransport, FakeTTS, pcm
from tests.media.test_speechkit import Recognizer, wait_until
from voice_gateway.events import CallEndReason
from voice_gateway.media.call import MediaCall, MediaSettings
from voice_gateway.media.providers.speechkit import SpeechKitSettings, SpeechKitSTT
from voice_gateway.logging_config import JsonFormatter


class RecoveryRecognizer(Recognizer):
    def __init__(self):
        super().__init__()
        self.failures = [grpc.StatusCode.UNAVAILABLE]
        self.final_before_error = False
        self.stall_retry = False
        self.retry_entered = asyncio.Event()
        self.fail_on_eou = False

    async def RecognizeStreaming(self, requests, context):
        index = len(self.requests)
        if index >= len(self.failures):
            self.retry_entered.set()
            if self.stall_retry:
                await asyncio.Future()
            async for response in super().RecognizeStreaming(requests, context):
                yield response
            return
        received = []
        self.requests.append(received)
        self.metadata.append(dict(context.invocation_metadata()))
        self.active += 1
        try:
            async for request in requests:
                received.append(request)
                event = request.WhichOneof("Event")
                if event == "chunk":
                    self.audio.set()
                    if self.fail_on_eou:
                        continue
                if event == "chunk" or (self.fail_on_eou and event == "eou"):
                    if self.final_before_error:
                        yield p.StreamingResponse(
                            final=p.AlternativeUpdate(
                                alternatives=[p.Alternative(text="discard me")]
                            )
                        )
                    if self.failures[index] is None:
                        return
                    await context.abort(
                        self.failures[index], "secret provider details fixture-secret"
                    )
        finally:
            self.active -= 1
            self.closed.set()


@pytest.fixture
async def recovery_provider():
    server = grpc.aio.server()
    fake = RecoveryRecognizer()
    service.add_RecognizerServicer_to_server(fake, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channels = []

    def connect():
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        channels.append(channel)
        return channel, service.RecognizerStub(channel)

    yield fake, connect
    for channel in channels:
        await channel.close()
    await server.stop(None)


@pytest.mark.parametrize(
    "failure",
    [
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.INTERNAL,
        None,
    ],
)
async def test_transient_stt_error_keeps_media_call_and_admits_one_complete_utterance(
    recovery_provider,
    failure,
):
    fake, connect = recovery_provider
    fake.failures = [failure]
    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    transport, port = FakeTransport(), FakeConversationPort()
    call = MediaCall(
        transport,
        stt,
        FakeTTS(),
        port,
        settings=MediaSettings(
            turn_silence_seconds=0, continuation_seconds=0, numeric_continuation_seconds=0
        ),
    )
    task = asyncio.create_task(call.run())
    try:
        await wait_until(lambda: port.started)
        await transport.incoming.put(pcm(1800, 200))
        await asyncio.wait_for(fake.closed.wait(), 1)
        await transport.incoming.put(pcm(1800, 100))
        await transport.incoming.put(pcm(0, 500))
        await wait_until(lambda: task.done() or port.utterances)
        assert not task.done(), "Transient recognition failure closed the whole call"
        assert [event.text for event in port.utterances] == ["fixture 1"]
        assert len(fake.requests) == 2
        replay = fake.requests[1]
        assert (
            replay[0].session_options.recognition_model.audio_processing_type
            == p.RecognitionModelOptions.FULL_DATA
        )
        assert b"".join(r.chunk.data for r in replay if r.WhichOneof("Event") == "chunk") == pcm(
            1800, 300
        ) + pcm(0, 500)
        await transport.incoming.put(None)
        await task
        assert port.ended[0].reason == CallEndReason.DISCONNECTED
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert not stt._tasks and not stt._rpcs and transport.closed


async def completed_utterance(stt, amplitude=1800):
    await stt.feed_audio(pcm(amplitude, 200))
    await stt.feed_audio(pcm(0, 500))


@pytest.mark.parametrize("final_before_error", [False, True])
async def test_recovery_keeps_local_order_and_discards_failed_rpc_text(
    recovery_provider,
    final_before_error,
):
    fake, connect = recovery_provider
    fake.final_before_error = final_before_error
    finals = []

    async def final(text):
        finals.append(text)

    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream(on_final=final)
    try:
        await stt.feed_audio(pcm(1800, 200))
        await asyncio.wait_for(fake.closed.wait(), 1)
        assert not finals
        await stt.feed_audio(pcm(0, 500))
        await completed_utterance(stt, 2000)
        await wait_until(lambda: len(finals) == 2)
        assert len(fake.requests) == 3
        retry_index = next(
            i
            for i, requests in enumerate(fake.requests)
            if requests[0].session_options.recognition_model.audio_processing_type
            == p.RecognitionModelOptions.FULL_DATA
        )
        other_index = next(i for i in (1, 2) if i != retry_index)
        assert finals == [f"fixture {retry_index}", f"fixture {other_index}"]
        assert "discard me" not in finals and not stt._error
    finally:
        await stt.abort_stream()
    assert not stt._tasks and not stt._rpcs


@pytest.mark.parametrize(
    "status",
    [
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.UNKNOWN,
    ],
)
async def test_nontransient_status_has_no_retry_and_logs_only_safe_categories(
    recovery_provider,
    caplog,
    status,
):
    fake, connect = recovery_provider
    fake.failures = [status]
    # Consume the request before aborting so the Python/C-core write/read race
    # cannot replace the server's intentional authentication/quota status with
    # a transport INTERNAL. Early technical failures are covered separately.
    fake.fail_on_eou = True
    caplog.set_level(logging.INFO)
    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream()
    try:
        await completed_utterance(stt)
        await wait_until(lambda: stt._error)
        with pytest.raises(RuntimeError, match="unavailable"):
            await stt.feed_audio(pcm(0, 20))
        assert len(fake.requests) == 1
        errors = [r for r in caplog.records if getattr(r, "event", None) == "stt_request_error"]
        assert len(errors) == 1
        formatted = JsonFormatter().format(errors[0])
        row = json.loads(formatted)
        assert row["error_type"] == "AioRpcError"
        assert row["provider_status"] == status.name
        assert row["attempt"] == 1 and row["retryable"] is False
        assert "fixture-secret" not in caplog.text + formatted
        assert "secret provider details" not in caplog.text + formatted
    finally:
        await stt.abort_stream()


async def test_second_failure_exhausts_attempts_without_late_or_duplicate_final(recovery_provider):
    fake, connect = recovery_provider
    fake.failures = [grpc.StatusCode.UNAVAILABLE] * 2
    finals = []

    async def final(text):
        finals.append(text)

    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream(on_final=final)
    try:
        await completed_utterance(stt)
        await wait_until(lambda: stt._error)
        assert len(fake.requests) == 2 and not finals
        with pytest.raises(RuntimeError, match="unavailable"):
            await stt.feed_audio(pcm(0, 20))
    finally:
        await stt.abort_stream()
    assert not stt._tasks and not stt._rpcs


async def test_hangup_before_local_endpoint_never_retries_or_flushes_audio(recovery_provider):
    fake, connect = recovery_provider
    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream()
    await stt.feed_audio(pcm(1800, 200))
    pending = stt._current
    await asyncio.wait_for(fake.closed.wait(), 1)
    await stt.abort_stream()
    assert len(fake.requests) == 1
    assert not pending.retained and not stt._tasks and not stt._rpcs


async def test_hangup_during_recovery_cancels_rpc_and_erases_transient_pcm(recovery_provider):
    fake, connect = recovery_provider
    fake.stall_retry = True
    finals = []

    async def final(text):
        finals.append(text)

    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream(on_final=final)
    await stt.feed_audio(pcm(1800, 200))
    pending = stt._current
    await asyncio.wait_for(fake.closed.wait(), 1)
    await stt.feed_audio(pcm(0, 500))
    await asyncio.wait_for(fake.retry_entered.wait(), 1)
    await stt.abort_stream()
    assert not finals and not pending.retained and not stt._tasks and not stt._rpcs


async def test_hangup_after_endpoint_stops_backoff_before_a_second_rpc(recovery_provider):
    fake, connect = recovery_provider
    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream()
    await stt.feed_audio(pcm(1800, 200))
    pending = stt._current
    await asyncio.wait_for(fake.closed.wait(), 1)
    await stt.feed_audio(pcm(0, 500))
    await stt.abort_stream()
    assert len(fake.requests) == 1 and not pending.retained
    assert not stt._tasks and not stt._rpcs


async def test_recovery_does_not_fill_an_unconsumed_live_queue(recovery_provider):
    fake, connect = recovery_provider
    finals = []

    async def final(text):
        finals.append(text)

    stt = SpeechKitSTT(
        SpeechKitSettings("fixture-secret", max_audio_queue_frames=32), connection_factory=connect
    )
    await stt.start_stream(on_final=final)
    try:
        await stt.feed_audio(pcm(1800, 200))
        pending = stt._current
        await wait_until(lambda: pending.recovering)
        await stt.feed_audio(pcm(1800, 2000))
        await stt.feed_audio(pcm(0, 500))
        await wait_until(lambda: finals)
        assert finals == ["fixture 1"] and not stt._error
        replay = b"".join(
            r.chunk.data for r in fake.requests[1] if r.WhichOneof("Event") == "chunk"
        )
        assert replay == pcm(1800, 2200) + pcm(0, 500)
    finally:
        await stt.abort_stream()


async def test_final_deadline_is_not_extended_by_recovery(recovery_provider):
    fake, connect = recovery_provider
    stt = SpeechKitSTT(
        SpeechKitSettings("fixture-secret", final_timeout=0.04), connection_factory=connect
    )
    await stt.start_stream()
    try:
        await stt.feed_audio(pcm(1800, 200))
        await asyncio.wait_for(fake.closed.wait(), 1)
        await stt.feed_audio(pcm(0, 500))
        await wait_until(lambda: stt._error)
        assert len(fake.requests) == 1
    finally:
        await stt.abort_stream()


async def test_callback_failure_cannot_retry_an_already_recognized_utterance(recovery_provider):
    fake, connect = recovery_provider
    fake.failures = []
    delivered = []

    async def final(text):
        delivered.append(text)
        raise RuntimeError("callback failed after accepting text")

    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream(on_final=final)
    try:
        await completed_utterance(stt)
        await wait_until(lambda: stt._error)
        assert delivered == ["fixture 0"] and len(fake.requests) == 1
    finally:
        await stt.abort_stream()


async def test_completed_utterance_is_not_included_in_later_recovery(recovery_provider):
    fake, connect = recovery_provider
    fake.failures = []
    finals = []

    async def final(text):
        finals.append(text)

    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
    await stt.start_stream(on_final=final)
    try:
        await completed_utterance(stt, 1000)
        await wait_until(lambda: len(finals) == 1)
        fake.failures = [None, grpc.StatusCode.UNAVAILABLE]
        await completed_utterance(stt, 2000)
        await wait_until(lambda: len(finals) == 2)
        assert finals == ["fixture 0", "fixture 2"]
        replay = b"".join(
            r.chunk.data for r in fake.requests[2] if r.WhichOneof("Event") == "chunk"
        )
        assert replay == pcm(2000, 200) + pcm(0, 500)
    finally:
        await stt.abort_stream()


async def test_five_independent_speech_streams_recover_once_without_cross_call_finals(
    recovery_provider,
):
    fake, connect = recovery_provider
    fake.failures = [grpc.StatusCode.UNAVAILABLE] * 5
    results = [[] for _ in range(5)]
    streams = []
    for index in range(5):

        async def final(text, index=index):
            results[index].append(text)

        stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"), connection_factory=connect)
        await stt.start_stream(on_final=final)
        streams.append(stt)
    try:
        await asyncio.gather(
            *(stt.feed_audio(pcm(1800 + i * 100, 200)) for i, stt in enumerate(streams))
        )
        await wait_until(lambda: len(fake.requests) == 5 and fake.active == 0)
        await asyncio.gather(*(stt.feed_audio(pcm(0, 500)) for stt in streams))
        await wait_until(lambda: all(len(items) == 1 for items in results))
        assert len(fake.requests) == 10
        assert len({items[0] for items in results}) == 5
        assert all(not stt._error for stt in streams)
        for i, items in enumerate(results):
            rpc_index = int(items[0].split()[1])
            audio = b"".join(
                r.chunk.data for r in fake.requests[rpc_index] if r.WhichOneof("Event") == "chunk"
            )
            assert audio == pcm(1800 + i * 100, 200) + pcm(0, 500)
    finally:
        await asyncio.gather(*(stt.abort_stream() for stt in streams))
    assert all(not stt._tasks and not stt._rpcs for stt in streams)


def test_recovery_pcm_is_bounded_and_excluded_from_object_repr():
    from voice_gateway.media.providers.speechkit import _Utterance

    stt = SpeechKitSTT(SpeechKitSettings("fixture-secret"))
    stt.vad.max_buffer_bytes = 640
    utterance = _Utterance(asyncio.Queue(32))
    stt._enqueue(utterance, pcm(1800, 40))
    with pytest.raises(RuntimeError, match="recovery audio"):
        stt._enqueue(utterance, pcm(1800, 20))
    assert len(utterance.retained) == 640 and utterance.audio.qsize() == 1
    assert "retained=" not in repr(utterance)
