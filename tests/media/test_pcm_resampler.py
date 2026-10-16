"""Telephone-band fidelity, duration, streaming latency, bounds and teardown."""

import asyncio
import math
import random
import struct
from contextlib import aclosing

import pytest

from voice_gateway.media.providers.pcm_resampler import (
    TelephoneResampler,
    resample_pcm_stream,
)


def tone(frequency, samples=24000, amplitude=12000):
    values = [
        round(amplitude * math.sin(2 * math.pi * frequency * n / 24000)) for n in range(samples)
    ]
    return struct.pack("<" + "h" * samples, *values)


def convert(data, size):
    converter = TelephoneResampler()
    result = b"".join(converter.feed(data[n : n + size]) for n in range(0, len(data), size))
    return result + converter.feed(b"", final=True)


@pytest.mark.parametrize("frequency", [300, 1000, 2500, 3400])
def test_telephone_band_gain_and_duration(frequency):
    result = convert(tone(frequency, samples=4800), 777)
    assert len(result) == 3200
    samples = struct.unpack("<1600h", result)[100:-100]
    rms = math.sqrt(sum(n * n for n in samples) / len(samples))
    assert abs(20 * math.log10(rms / (12000 / math.sqrt(2)))) < 0.2


@pytest.mark.parametrize("frequency", [4000, 4100, 4500, 6000, 10000])
def test_above_nyquist_does_not_alias_into_phone_audio(frequency):
    result = convert(tone(frequency, samples=4800), 1024)
    samples = struct.unpack("<1600h", result)[100:-100]
    rms = math.sqrt(sum(n * n for n in samples) / len(samples))
    assert rms < 12000 / math.sqrt(2) * 10 ** (-60 / 20)


@pytest.mark.parametrize("samples", [1, 2, 3, 119, 120, 121, 6000, 6001, 6002])
def test_no_lost_tail_or_chunk_boundary_changes(samples):
    rng = random.Random(42)
    data = struct.pack("<" + "h" * samples, *(rng.randint(-20000, 20000) for _ in range(samples)))
    expected = convert(data, 65536)
    assert len(expected) == (samples + 2) // 3 * 2
    assert convert(data, 319) == expected
    assert convert(data, 2) == expected


def test_stream_edge_and_clipping_are_safe():
    data = struct.pack("<600h", *([32767] * 300 + [-32768] * 300))
    output = struct.unpack("<200h", convert(data, 65536))
    assert max(output) == 32767 and min(output) == -32768
    converter = TelephoneResampler()
    converter.feed(b"", final=True)
    with pytest.raises(RuntimeError, match="closed"):
        converter.feed(b"xx")


async def test_first_telephone_frame_before_upstream_end_and_source_closed_on_barge_in():
    closed = asyncio.Event()

    async def source():
        try:
            yield tone(1000, samples=600)  # Only 25 ms, not ffmpeg's startup buffer.
            await asyncio.Future()
        finally:
            closed.set()

    async with aclosing(resample_pcm_stream(source())) as stream:
        output = await asyncio.wait_for(anext(stream), 0.5)
        assert len(output) >= 320
    assert closed.is_set()


@pytest.mark.parametrize(
    "kind", ["partial_sample", "empty", "input_limit", "output_limit", "chunk_limit"]
)
async def test_invalid_audio_or_limits_fail_closed_and_close_source(kind):
    closed = asyncio.Event()

    async def source():
        try:
            yield {"partial_sample": b"x", "empty": b"", "chunk_limit": b"x" * 65537}.get(
                kind, tone(1000, samples=600)
            )
        finally:
            closed.set()

    kwargs = (
        {"max_input_bytes": 10}
        if kind == "input_limit"
        else ({"max_output_bytes": 10} if kind == "output_limit" else {})
    )
    with pytest.raises(ValueError):
        async with aclosing(resample_pcm_stream(source(), **kwargs)) as stream:
            _ = [chunk async for chunk in stream]
    assert closed.is_set()


async def test_upstream_timeout_cancels_and_closes_source():
    closed = asyncio.Event()

    async def source():
        try:
            await asyncio.Future()
            yield b""
        finally:
            closed.set()

    with pytest.raises(TimeoutError):
        await anext(resample_pcm_stream(source(), timeout=0.01))
    assert closed.is_set()
