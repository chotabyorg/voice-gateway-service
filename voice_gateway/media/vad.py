"""Energy VAD/end-of-speech and bounded preroll for batch STT."""

from dataclasses import dataclass
from .audio import PCM, rms, duration_ms


@dataclass(frozen=True)
class VADSettings:
    threshold: int = 500
    release_ratio: float = 0.6
    silence_end_ms: int = 500
    min_speech_ms: int = 80
    preroll_ms: int = 200
    max_utterance_ms: int = 30000

    def __post_init__(self):
        if (
            self.threshold < 1
            or not 0.5 <= self.release_ratio <= 1
            or self.min_speech_ms <= 0
            or self.silence_end_ms <= 0
            or self.preroll_ms < 0
            or self.max_utterance_ms < self.min_speech_ms
        ):
            raise ValueError("Invalid VAD settings")


class EnergyVAD:
    def __init__(self, settings=VADSettings()):
        self.settings = settings
        self.reset()

    def reset(self):
        self.buffer = []
        self.buffer_ms = self.silence_ms = self.speech_ms = 0.0
        self.has_speech = False
        self.preroll = bytearray()

    def feed(self, chunk):
        """Return True when the buffered utterance is ready; caller must take it."""
        if not chunk:
            return False
        energy, milliseconds = rms(chunk), duration_ms(chunk)
        threshold = self.settings.threshold
        if self.speech_ms >= self.settings.min_speech_ms:
            threshold *= self.settings.release_ratio
        if energy >= threshold:
            if not self.has_speech and self.preroll:
                self.buffer.append(bytes(self.preroll))
                self.buffer_ms += duration_ms(bytes(self.preroll))
                self.preroll.clear()
            self.buffer.append(chunk)
            self.buffer_ms += milliseconds
            self.speech_ms += milliseconds
            self.silence_ms = 0
            self.has_speech = True
        elif self.has_speech:
            self.buffer.append(chunk)
            self.buffer_ms += milliseconds
            self.silence_ms += milliseconds
        else:
            self.preroll.extend(chunk)
            limit = self.settings.preroll_ms * PCM.bytes_per_second // 1000
            if len(self.preroll) > limit:
                del self.preroll[: len(self.preroll) - limit]
        if self.has_speech and self.silence_ms >= self.settings.silence_end_ms:
            if self.speech_ms < self.settings.min_speech_ms:
                self.reset()
                return False
            return True
        return self.buffer_ms >= self.settings.max_utterance_ms

    def take(self):
        pcm = b"".join(self.buffer)
        self.reset()
        return pcm
