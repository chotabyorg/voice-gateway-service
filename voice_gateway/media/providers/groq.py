"""Groq Whisper adapter with explicit settings and injectable SDK client."""

from dataclasses import dataclass, field
from ..audio import pcm_to_wav, PCM
from ..batch_stt import BatchSTT


@dataclass(frozen=True)
class GroqSTTSettings:
    api_key: str = field(repr=False)
    model: str = "whisper-large-v3"
    language: str = ""
    timeout: float = 30
    max_retries: int = 1

    def __post_init__(self):
        if not self.api_key or not self.model or self.timeout <= 0 or self.max_retries < 0:
            raise ValueError("Invalid Groq STT settings")


class GroqSTT(BatchSTT):
    def __init__(self, settings, *, client_factory=None, on_audio_usage=None, **kwargs):
        super().__init__(**kwargs)
        self.settings = settings
        self.client_factory = client_factory
        self.on_audio_usage = on_audio_usage
        self._client = None
        self._model, self._language = settings.model, settings.language

    async def _open(self):
        factory = self.client_factory
        if factory is None:
            from groq import AsyncGroq

            factory = AsyncGroq
        self._client = factory(
            api_key=self.settings.api_key,
            timeout=self.settings.timeout,
            max_retries=self.settings.max_retries,
        )
        self.emit(
            "stt_stream_started",
            provider="groq_whisper",
            model=self._model,
            language=self._language,
        )

    async def _transcribe(self, pcm):
        if self._client is None:
            return ""
        if self.on_audio_usage:
            self.on_audio_usage(len(pcm) / PCM.bytes_per_second)
        options = {"language": self._language} if self._language else {}
        result = await self._client.audio.transcriptions.create(
            file=("utterance.wav", pcm_to_wav(pcm), "audio/wav"),
            model=self._model,
            response_format="json",
            temperature=0,
            **options,
        )
        return (result.text or "").strip()

    async def _close(self):
        client, self._client = self._client, None
        if client is not None:
            await client.close()
