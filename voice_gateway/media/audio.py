"""Canonical PCM helpers, without audioop or provider/product dependencies."""

from array import array
from dataclasses import dataclass
from io import BytesIO
from math import isqrt
import sys
import wave


@dataclass(frozen=True)
class PCMFormat:
    sample_rate: int = 8000
    sample_width: int = 2
    channels: int = 1
    frame_ms: int = 20

    @property
    def bytes_per_second(self):
        return self.sample_rate * self.sample_width * self.channels

    @property
    def frame_bytes(self):
        return self.bytes_per_second * self.frame_ms // 1000


PCM = PCMFormat()


def validate_pcm(chunk):
    if not isinstance(chunk, bytes) or len(chunk) % 2:
        raise ValueError("Audio must be bytes containing complete PCM16 samples")


def samples(chunk):
    validate_pcm(chunk)
    values = array("h")
    values.frombytes(chunk)
    if sys.byteorder != "little":
        values.byteswap()
    return values


def rms(chunk):
    values = samples(chunk)
    return isqrt(sum(v * v for v in values) // len(values)) if values else 0


def duration_ms(chunk):
    validate_pcm(chunk)
    return len(chunk) * 1000 / PCM.bytes_per_second


def pcm_to_wav(chunk):
    validate_pcm(chunk)
    buffer = BytesIO()
    with wave.open(buffer, "wb") as output:
        output.setparams((1, 2, 8000, 0, "NONE", "not compressed"))
        output.writeframes(chunk)
    return buffer.getvalue()


def amplify_pcm(chunk, gain):
    """Fixed, opt-in input gain; preserve silence and saturate PCM16 safely."""
    validate_pcm(chunk)
    if not isinstance(gain, int) or not 1 <= gain <= 32:
        raise ValueError("Invalid input gain")
    if gain == 1:
        return chunk
    output = array("h", (max(-32768, min(32767, v * gain)) for v in samples(chunk)))
    if sys.byteorder != "little":
        output.byteswap()
    return output.tobytes()


def mix_pcm(left, right):
    size = max(len(left), len(right)) // 2 * 2
    a, b = samples(left[:size].ljust(size, b"\0")), samples(right[:size].ljust(size, b"\0"))
    output = array("h", (max(-32768, min(32767, x + y)) for x, y in zip(a, b)))
    if sys.byteorder != "little":
        output.byteswap()
    return output.tobytes()
