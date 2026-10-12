"""Provider-neutral interruption detector, including optional echo calibration."""

from dataclasses import dataclass
from .audio import duration_ms, rms, PCM


@dataclass(frozen=True)
class InterruptionSettings:
    required_ms: int = 100
    min_rms: int = 600
    preroll_ms: int = 200
    guarded: bool = False
    calibration_seconds: float = 0.2
    min_playback_seconds: float = 0.35
    echo_multiplier: float = 1.35

    def __post_init__(self):
        if (
            self.required_ms <= 0
            or self.min_rms < 1
            or self.preroll_ms < 0
            or self.calibration_seconds < 0
            or self.min_playback_seconds < 0
            or self.echo_multiplier < 1
        ):
            raise ValueError("Invalid interruption settings")


@dataclass(frozen=True)
class Interruption:
    chunks: tuple[bytes, ...]
    rms: int
    threshold: int


class InterruptionDetector:
    def __init__(self, settings=InterruptionSettings()):
        self.settings = settings
        self.echo_rms = 0
        self.reset()

    def reset(self):
        self.candidate_ms = 0.0
        self.chunks = []

    def feed(self, chunk, *, elapsed=0):
        energy, milliseconds = rms(chunk), duration_ms(chunk)
        policy = self.settings
        threshold = policy.min_rms
        if policy.guarded:
            if elapsed < policy.calibration_seconds:
                self.echo_rms = max(self.echo_rms, energy)
                self.reset()
                return None
            if elapsed < policy.min_playback_seconds:
                self.reset()
                return None
            threshold = max(threshold, int(self.echo_rms * policy.echo_multiplier))
            if energy < threshold:
                self.reset()
                return None
        self.chunks.append(chunk)
        if not policy.guarded:
            limit = (policy.required_ms + policy.preroll_ms) * PCM.bytes_per_second // 1000
            while len(self.chunks) > 1 and sum(map(len, self.chunks[1:])) >= limit:
                self.chunks.pop(0)
        self.candidate_ms = self.candidate_ms + milliseconds if energy >= threshold else 0
        if self.candidate_ms < policy.required_ms:
            return None
        event = Interruption(tuple(self.chunks), energy, threshold)
        self.reset()
        return event
