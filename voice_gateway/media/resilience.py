"""Provider wrappers with explicit timeouts and optional usage/event observers."""

import asyncio
from .audio import validate_pcm
from .interfaces import BaseSTT, BaseTTS


class ResilientSTT(BaseSTT):
    """Start-time fallback only. No silent mid-utterance provider switching."""

    def __init__(self, primary, fallback=None, *, emit=None):
        self._primary, self._fallback, self._active = primary, fallback, primary
        self.emit = emit or (lambda event, **values: None)

    async def start_stream(self, on_partial=None, on_final=None, on_eou=None):
        self._active = self._primary
        try:
            await self._active.start_stream(on_partial=on_partial, on_final=on_final, on_eou=on_eou)
        except Exception as exc:
            try:
                await self._primary.stop_stream()
            except Exception:
                pass
            if self._fallback is None:
                raise
            self.emit("stt_primary_start_failed_using_fallback", error=type(exc).__name__)
            self._active = self._fallback
            await self._active.start_stream(on_partial=on_partial, on_final=on_final, on_eou=on_eou)

    async def feed_audio(self, chunk):
        await self._active.feed_audio(chunk)

    async def stop_stream(self):
        await self._active.stop_stream()


class ResilientTTS(BaseTTS):
    def __init__(self, primary, fallback=None, *, timeout=30, emit=None, on_usage=None):
        if timeout <= 0:
            raise ValueError("TTS timeout must be positive")
        self._primary, self._fallback = primary, fallback
        self.timeout = timeout
        self.emit = emit or (lambda event, **values: None)
        self.on_usage = on_usage

    async def synthesize(self, text):
        if not text.strip():
            return b""
        try:
            return await self._synthesize(self._primary, text)
        except Exception as exc:
            if self._fallback is None:
                raise
            self.emit("tts_primary_failed_using_fallback", error=type(exc).__name__)
            return await self._synthesize(self._fallback, text)

    async def _synthesize(self, provider, text):
        if self.on_usage:
            self.on_usage(len(text))
        audio = await asyncio.wait_for(provider.synthesize(text), self.timeout)
        validate_pcm(audio)
        if not audio:
            raise ValueError("TTS must return nonempty PCM16 audio")
        return audio

    async def close(self):
        providers = list(dict.fromkeys(p for p in (self._primary, self._fallback) if p is not None))
        results = await asyncio.gather(*(p.close() for p in providers), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                self.emit("tts_close_failed", error=type(result).__name__)
