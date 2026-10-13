"""Asterisk AudioSocket: one TCP connection, UUID handshake, PCM16 LE / 8kHz.

Metadata enrichment, dialplan, AMI and product routing belong to the host app.
"""

import asyncio
import struct
import uuid
from .audio import validate_pcm
from .interfaces import CallTransport, TransportIdleTimeout

FRAME_HANGUP = 0x00
FRAME_UUID = 0x01
FRAME_AUDIO = 0x10
FRAME_ERROR = 0xFF


class AudioSocketTransport(CallTransport):
    def __init__(
        self, reader, writer, *, handshake_timeout=10, idle_timeout=30, max_frame_bytes=4096
    ):
        if not 1 <= max_frame_bytes <= 65535:
            raise ValueError("max_frame_bytes must be between 1 and 65535")
        if idle_timeout <= 0:
            raise ValueError("idle_timeout must be positive")
        self._reader, self._writer = reader, writer
        self.handshake_timeout = handshake_timeout
        self.idle_timeout = idle_timeout
        self.max_frame_bytes = max_frame_bytes
        self._closed = self._writer_closed = self._prepared = False

    async def prepare(self):
        if self._prepared:
            return
        frame = await asyncio.wait_for(self._read_frame(), self.handshake_timeout)
        if frame is None or frame[0] != FRAME_UUID or len(frame[1]) != 16:
            raise ValueError("Expected AudioSocket UUID handshake")
        self.call_id = str(uuid.UUID(bytes=frame[1]))
        self._prepared = True

    async def read_audio_chunk(self):
        while not self._closed:
            try:
                frame = await asyncio.wait_for(self._read_frame(), timeout=self.idle_timeout)
            except TimeoutError as exc:
                raise TransportIdleTimeout("AudioSocket frame idle timeout") from exc
            if frame is None:
                return None
            kind, payload = frame
            if kind == FRAME_AUDIO:
                validate_pcm(payload)
                return payload
            if kind == FRAME_UUID:
                if str(uuid.UUID(bytes=payload)) != self.call_id:
                    raise ValueError("AudioSocket UUID changed during the call")
            elif kind == FRAME_HANGUP:
                return None
            elif kind == FRAME_ERROR:
                raise ConnectionError("AudioSocket peer reported a media error")
            elif 0x11 <= kind <= 0x18:
                raise ValueError("AudioSocket sample rate unsupported; require PCM 8kHz")
            # DTMF and extension frames do not become audio.
        return None

    async def write_audio_chunk(self, chunk):
        validate_pcm(chunk)
        if len(chunk) > 65535:
            raise ValueError("AudioSocket payload exceeds frame limit")
        if self._closed:
            raise ConnectionError("AudioSocket is closed")
        try:
            self._writer.write(struct.pack("!BH", FRAME_AUDIO, len(chunk)) + chunk)
            await self._writer.drain()
        except ConnectionError:
            self._closed = True
            raise

    async def close(self):
        self._closed = True
        if self._writer_closed:
            return
        self._writer_closed = True
        self._writer.close()
        try:
            await self._writer.wait_closed()
        except ConnectionError:
            pass

    async def _read_frame(self):
        try:
            header = await self._reader.readexactly(3)
            kind, length = struct.unpack("!BH", header)
            if length > self.max_frame_bytes:
                raise ValueError("AudioSocket frame exceeds configured limit")
            return kind, await self._reader.readexactly(length)
        except (asyncio.IncompleteReadError, ConnectionError):
            return None
