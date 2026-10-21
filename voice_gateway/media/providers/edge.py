"""Bounded Edge TTS adapter; provider availability remains host policy."""

import asyncio
from dataclasses import dataclass
from contextlib import aclosing

from ..audio import validate_pcm
from ..completion import finish_owned_task
from ..interfaces import BaseTTS
from ..speech_text import prepare_tts_text

DEFAULT_VOICE = "ru-RU-SvetlanaNeural"
_PIPE_CHUNK = 65536


@dataclass(frozen=True)
class EdgeTTSSettings:
    voice: str = DEFAULT_VOICE
    rate: str = "+0%"
    pitch: str = "+0Hz"
    pronounce_booking_values: bool = False
    expand_byn: bool = False
    max_text_chars: int = 4000
    max_mp3_bytes: int = 8 * 1024 * 1024
    max_pcm_bytes: int = 8 * 1024 * 1024
    stream_timeout: float = 30
    decode_timeout: float = 30

    def __post_init__(self):
        if (
            not self.voice
            or self.max_text_chars < 1
            or self.max_mp3_bytes < 1
            or self.max_pcm_bytes < 2
            or self.max_mp3_bytes > 8 * 1024 * 1024
            or self.max_pcm_bytes > 8 * 1024 * 1024
            or self.stream_timeout <= 0
            or self.decode_timeout <= 0
        ):
            raise ValueError("Invalid Edge TTS bounds")


async def decode_mp3(
    mp3,
    *,
    executable="ffmpeg",
    timeout=30,
    max_input_bytes=8 * 1024 * 1024,
    max_output_bytes=8 * 1024 * 1024,
):
    """Decode with bounded pipes, timeout, kill-on-cancel, and deterministic reap."""
    if not mp3:
        return b""
    if not isinstance(mp3, bytes) or len(mp3) > max_input_bytes:
        raise ValueError("MP3 input exceeds configured limit")
    if timeout <= 0 or max_input_bytes < 1 or max_output_bytes < 2:
        raise ValueError("Invalid decoder bounds")
    command = [executable] if isinstance(executable, str) else list(executable)
    proc = await asyncio.create_subprocess_exec(
        *command,
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        "pipe:0",
        "-f",
        "s16le",
        "-acodec",
        "pcm_s16le",
        "-ar",
        "8000",
        "-ac",
        "1",
        "pipe:1",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=_PIPE_CHUNK,
    )
    output = bytearray()
    overflow = asyncio.Event()

    async def feed_stdin():
        try:
            proc.stdin.write(mp3)
            await proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            proc.stdin.close()

    async def read_stdout():
        while True:
            chunk = await proc.stdout.read(_PIPE_CHUNK)
            if not chunk:
                return
            if overflow.is_set() or len(output) + len(chunk) > max_output_bytes:
                overflow.set()
                if proc.returncode is None:
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
            else:
                output.extend(chunk)

    async def drain_stderr():
        # Drain the pipe to avoid child-process deadlock; never retain provider output.
        while await proc.stderr.read(_PIPE_CHUNK):
            pass

    tasks = [asyncio.create_task(coro) for coro in (feed_stdin(), read_stdout(), drain_stderr())]
    wait_task = asyncio.create_task(proc.wait(), name="ffmpeg-wait")
    completion = asyncio.gather(*tasks, wait_task)
    try:
        await asyncio.wait_for(asyncio.shield(completion), timeout)
    except TimeoutError:
        if proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
        await asyncio.wait_for(asyncio.shield(completion), 3)
        raise
    except asyncio.CancelledError as exc:
        if proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass

        async def finish_pipes():
            await completion

        cleanup = asyncio.create_task(finish_pipes(), name="ffmpeg-reap")
        await finish_owned_task(cleanup, cancellation=exc)
        raise
    if overflow.is_set():
        raise ValueError("Decoded PCM exceeds configured limit")
    if proc.returncode:
        raise RuntimeError("ffmpeg decode failed with exit code %s" % proc.returncode)
    decoded = bytes(output)
    validate_pcm(decoded)
    return decoded


class EdgeTTS(BaseTTS):
    def __init__(
        self, settings=EdgeTTSSettings(), *, communicate_factory=None, decoder=decode_mp3, emit=None
    ):
        self.settings = settings
        self.communicate_factory, self.decoder = communicate_factory, decoder
        self.emit = emit or (lambda event, **values: None)

    async def synthesize(self, text):
        text = (text or "").strip()
        if not text:
            return b""
        text = prepare_tts_text(
            text,
            booking_values=self.settings.pronounce_booking_values,
            expand_byn=self.settings.expand_byn,
        )
        if len(text) > self.settings.max_text_chars:
            raise ValueError("TTS text exceeds configured limit")
        mp3 = await self._synthesize_mp3(text)
        if not mp3:
            self.emit("tts_empty_mp3", text_len=len(text))
            return b""
        pcm = await self.decoder(
            mp3,
            max_input_bytes=self.settings.max_mp3_bytes,
            max_output_bytes=self.settings.max_pcm_bytes,
            timeout=self.settings.decode_timeout,
        )
        validate_pcm(pcm)
        if len(pcm) > self.settings.max_pcm_bytes:
            raise ValueError("TTS PCM exceeds configured limit")
        self.emit(
            "tts_done",
            text_len=len(text),
            mp3_bytes=len(mp3),
            pcm_bytes=len(pcm),
            voice=self.settings.voice,
        )
        return pcm

    async def _synthesize_mp3(self, text):
        factory = self.communicate_factory
        if factory is None:
            from edge_tts import Communicate

            factory = Communicate
        client = factory(
            text, voice=self.settings.voice, rate=self.settings.rate, pitch=self.settings.pitch
        )
        chunks, total = [], 0
        try:
            async with aclosing(client.stream()) as stream:
                async with asyncio.timeout(self.settings.stream_timeout):
                    async for chunk in stream:
                        if chunk.get("type") != "audio":
                            continue
                        data = chunk.get("data", b"")
                        if not isinstance(data, bytes):
                            raise ValueError("Edge TTS returned a non-byte audio chunk")
                        total += len(data)
                        if total > self.settings.max_mp3_bytes:
                            raise ValueError("TTS MP3 exceeds configured limit")
                        chunks.append(data)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.emit("tts_edge_stream_error", error_type=type(exc).__name__)
            raise
        return b"".join(chunks)
