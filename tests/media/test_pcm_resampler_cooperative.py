"""Offline tests against actual selected source; no provider or transport traffic."""

import asyncio
import random
from contextlib import aclosing

import pytest

from voice_gateway.media.providers import pcm_resampler as mod


async def collect(parts, **kwargs):
    async def source():
        for part in parts:
            yield part

    async with aclosing(mod.resample_pcm_stream(source(), **kwargs)) as stream:
        return [part async for part in stream]


@pytest.mark.parametrize(
    "cuts",
    [[65536], [1, 0, 65535], [959, 0, 960, 961, 636], [1, 2, 3, 237, 239, 240, 241, 480, 960]],
)
async def test_exact_bytes_and_outgoing_boundaries(cuts):
    data = random.Random(19).randbytes(65536)
    parts, offset = [], 0
    for length in cuts:
        parts.append(data[offset : offset + length])
        offset += length
    parts.append(data[offset:])
    reference = mod.TelephoneResampler()
    expected = [reference.feed(part) for part in parts]
    expected.append(reference.feed(b"", final=True))
    assert await collect(parts) == [part for part in expected if part]


async def test_yields_to_ready_callback_before_large_output(monkeypatch):
    real_feed = mod.TelephoneResampler.feed
    seen = []

    def feed(self, data, **kwargs):
        if not seen:
            asyncio.get_running_loop().call_soon(seen.append, "callback")
        return real_feed(self, data, **kwargs)

    monkeypatch.setattr(mod.TelephoneResampler, "feed", feed)

    async def source():
        yield b"\0" * 65536

    async with aclosing(mod.resample_pcm_stream(source())) as stream:
        assert await anext(stream)
        assert seen == ["callback"]


async def test_cancel_during_large_chunk_closes_source_before_output(monkeypatch):
    closed, emitted = [], []
    calls = []
    tasks_before = asyncio.all_tasks()
    task = None
    real_feed = mod.TelephoneResampler.feed

    def feed(self, data, **kwargs):
        calls.append(len(data))
        asyncio.get_running_loop().call_soon(task.cancel)
        return real_feed(self, data, **kwargs)

    monkeypatch.setattr(mod.TelephoneResampler, "feed", feed)

    async def source():
        try:
            yield b"\0" * 65536
        finally:
            closed.append(True)

    async def consume():
        async with aclosing(mod.resample_pcm_stream(source())) as stream:
            emitted.append(await anext(stream))
            await asyncio.sleep(0)

    task = asyncio.create_task(consume())
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed == [True]
    assert emitted == []
    assert calls and sum(calls) < 65536
    assert max(calls) <= 960
    assert asyncio.all_tasks() == tasks_before


@pytest.mark.parametrize("phase", ["inside_feed", "while_yielded"])
@pytest.mark.parametrize("final_only", [False, True])
async def test_expiry_during_compute_or_yield_never_emits(monkeypatch, phase, final_only):
    loop = asyncio.get_running_loop()
    now = [loop.time()]
    monkeypatch.setattr(loop, "time", lambda: now[0])

    def expire():
        now[0] += 20

    real_feed = mod.TelephoneResampler.feed

    def feed(self, data, **kwargs):
        result = real_feed(self, data, **kwargs)
        if not final_only or kwargs.get("final"):
            if phase == "inside_feed":
                expire()
            else:
                loop.call_soon(expire)
        return result

    monkeypatch.setattr(mod.TelephoneResampler, "feed", feed)
    closed = []

    async def source():
        try:
            yield b"\0" * (2 if final_only else 960)
        finally:
            closed.append(True)

    with pytest.raises(TimeoutError):
        async with aclosing(mod.resample_pcm_stream(source(), timeout=10)) as stream:
            await anext(stream)
    assert closed == [True]


async def test_generator_does_not_hold_timeout_over_consumer(monkeypatch):
    loop = asyncio.get_running_loop()
    now = [loop.time()]
    monkeypatch.setattr(loop, "time", lambda: now[0])
    closed = []

    async def source():
        try:
            yield b"\0" * 960
            yield b"\0" * 960
        finally:
            closed.append(True)

    async with aclosing(mod.resample_pcm_stream(source(), timeout=10)) as stream:
        assert await anext(stream)
        now[0] += 20
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        with pytest.raises(TimeoutError):
            await anext(stream)
    assert closed == [True]


@pytest.mark.parametrize(
    "parts,kwargs,message",
    [
        ([b"", b""], {}, "empty PCM"),
        ([b"x" * 961], {}, "Incomplete"),
        ([b"x" * 65537], {}, "Invalid PCM resampler chunk"),
        ([b"x" * 65536], {"max_input_bytes": 65535}, "Encoded audio"),
        ([b"x" * 65536], {"max_output_bytes": 2}, "Decoded audio"),
    ],
)
async def test_bounds_and_empty_or_odd_inputs(parts, kwargs, message):
    with pytest.raises(ValueError, match=message):
        await collect(parts, **kwargs)
