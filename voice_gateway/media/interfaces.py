"""Per-call PCM interfaces; these are runtime ports, not platform Call models."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Awaitable, Callable

TextCallback = Callable[[str], Awaitable[None]]
EndCallback = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class STTCapabilities:
    streaming_transcription: bool = False
    partial_results: bool = False
    cancellation: bool = True
    local_speech_timestamps: bool = True
    input_pcm_rate: int = 8000


@dataclass(frozen=True)
class TTSCapabilities:
    streaming_synthesis: bool = False
    cancellation: bool = True
    output_pcm_rate: int = 8000


class TransportIdleTimeout(TimeoutError):
    """No transport frame arrived before its configured inactivity deadline."""


class CallTransport(ABC):
    """One duplex call. PCM16 signed little endian, 8000Hz, mono.

    write completion means handed to the transport, not heard by the caller.
    prepare/close must be safe to repeat. Metadata is optional local correlation.
    """

    call_id: str = ""

    async def prepare(self) -> None:
        pass

    @abstractmethod
    async def read_audio_chunk(self) -> bytes | None:
        """None = call EOF; b'' = no audio; idle deadlines raise TransportIdleTimeout.

        No chunk contains half a PCM sample.
        """

    @abstractmethod
    async def write_audio_chunk(self, chunk: bytes) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass

    async def clear_output(self) -> None:
        """Discard buffered playback if supported; cannot retract already sent audio."""


class BaseSTT(ABC):
    capabilities = STTCapabilities()
    uses_speech_classifier: bool = False
    last_speech_at: float | None = None
    final_speech_at: float | None = None

    @abstractmethod
    async def start_stream(
        self,
        on_partial: TextCallback | None = None,
        on_final: TextCallback | None = None,
        on_eou: EndCallback | None = None,
    ) -> None:
        pass

    @abstractmethod
    async def feed_audio(self, chunk: bytes) -> None:
        pass

    @abstractmethod
    async def stop_stream(self) -> None:
        """Drain or cancel outstanding requests and release provider resources."""

    async def abort_stream(self) -> None:
        """Stop an ended call; adapters may discard unusable pending results."""
        await self.stop_stream()


class BaseTTS(ABC):
    capabilities = TTSCapabilities()

    @abstractmethod
    async def synthesize(self, text: str) -> bytes:
        """PCM16 8000Hz mono. Provider conversion belongs in the adapter."""

    async def synthesize_stream(self, text: str):
        """Bounded chunk view for batch adapters; not native streaming synthesis."""
        audio = await self.synthesize(text)
        for offset in range(0, len(audio), 65536):
            yield audio[offset : offset + 65536]

    def prewarm(self) -> None:
        """Optional connection preparation; resource ownership stays in the adapter."""

    async def close(self) -> None:
        pass
