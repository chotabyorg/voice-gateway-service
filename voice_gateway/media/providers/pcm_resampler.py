"""Bounded 24 kHz -> telephone 8 kHz PCM FIR; no subprocess or audio retention.

The 241-tap Blackman-windowed sinc has 5 ms lookahead. Its 3.6 kHz
cutoff preserves the 300-3400 Hz telephone band and suppresses aliases above
4 kHz. Padding at stream edges preserves ceil(input_samples / 3) samples.
"""

import asyncio
import math
import sys
from array import array
from contextlib import aclosing

from ..completion import deadline_timeout

_HALF = 120
_TAPS = 2 * _HALF + 1
_CUTOFF = 3600 / 24000


def _coefficients():
    taps = []
    for index in range(_TAPS):
        distance = index - _HALF
        sinc = (
            2 * _CUTOFF
            if distance == 0
            else math.sin(2 * math.pi * _CUTOFF * distance) / (math.pi * distance)
        )
        window = (
            0.42
            - 0.5 * math.cos(2 * math.pi * index / (_TAPS - 1))
            + 0.08 * math.cos(4 * math.pi * index / (_TAPS - 1))
        )
        taps.append(sinc * window)
    scale = sum(taps)
    return tuple(value / scale for value in taps)


_FILTER = _coefficients()


class TelephoneResampler:
    """Single-stream converter. Retains at most one bounded input chunk + FIR tail."""

    def __init__(self):
        self._samples = [0] * _HALF
        self._position = 0
        self._input_samples = self._output_samples = 0
        self._carry = b""
        self._closed = False

    def feed(self, chunk: bytes, *, final=False) -> bytes:
        if self._closed:
            raise RuntimeError("PCM resampler is closed")
        if not isinstance(chunk, bytes) or len(chunk) > 65536:
            raise ValueError("Invalid PCM resampler chunk")
        data = self._carry + chunk
        end = len(data) // 2 * 2
        self._carry = data[end:]
        incoming = array("h")
        incoming.frombytes(data[:end])
        if sys.byteorder != "little":
            incoming.byteswap()
        self._input_samples += len(incoming)
        self._samples.extend(incoming)
        if final:
            self._closed = True
            if self._carry:
                raise ValueError("Incomplete raw TTS PCM sample")
            self._samples.extend([0] * _HALF)
        output = array("h")
        limit = (self._input_samples + 2) // 3
        while self._position + _TAPS <= len(self._samples) and self._output_samples < limit:
            value = sum(
                sample * coefficient
                for sample, coefficient in zip(
                    self._samples[self._position : self._position + _TAPS], _FILTER
                )
            )
            output.append(max(-32768, min(32767, round(value))))
            self._position += 3
            self._output_samples += 1
        if self._position:
            del self._samples[: self._position]
            self._position = 0
        if final:
            self._samples.clear()
        if sys.byteorder != "little":
            output.byteswap()
        return output.tobytes()


async def resample_pcm_stream(
    chunks,
    *,
    max_input_bytes=3_000_000,
    max_output_bytes=1_920_000,
    timeout=120,
    input_pcm_rate=24000,
):
    if input_pcm_rate != 24000 or timeout <= 0 or max_input_bytes < 1 or max_output_bytes < 2:
        raise ValueError("Invalid PCM resampler bounds")
    deadline = asyncio.get_running_loop().time() + timeout
    converter = TelephoneResampler()
    received = produced = 0
    async with aclosing(chunks) as source:
        while True:
            async with deadline_timeout(deadline):
                try:
                    chunk = await anext(source)
                except StopAsyncIteration:
                    chunk = None
            if chunk is not None:
                if not isinstance(chunk, bytes) or len(chunk) > 65536:
                    raise ValueError("Invalid PCM resampler chunk")
                received += len(chunk)
                if received > max_input_bytes:
                    raise ValueError("Encoded audio exceeds configured limit")
            pcm = converter.feed(chunk or b"", final=chunk is None)
            produced += len(pcm)
            if produced > max_output_bytes:
                raise ValueError("Decoded audio exceeds configured limit")
            if pcm:
                yield pcm
            if chunk is None:
                if not produced:
                    raise ValueError("Decoder returned empty PCM")
                break
            # Bounded CPU work must not starve input/VAD or barge-in callbacks.
            await asyncio.sleep(0)
