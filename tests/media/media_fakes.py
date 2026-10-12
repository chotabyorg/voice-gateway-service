"""Deterministic PCM and media fakes; no product factories or real provider SDKs."""

import asyncio
import struct
from voice_gateway.media.interfaces import CallTransport, BaseSTT, BaseTTS
from voice_gateway.events import AuthorizedSpeech


def pcm(amplitude=1000, milliseconds=20):
    return struct.pack("<h", amplitude) * (8 * milliseconds)


def utterance_fixture():
    """Quiet lead-in, speech, pause, speech, end-of-speech; all PCM 8k mono."""
    return [pcm(100, 100), pcm(1000, 100), pcm(0, 100), pcm(2000, 100), pcm(0, 500)]


class FakeTransport(CallTransport):
    def __init__(self):
        self.incoming = asyncio.Queue(maxsize=64)
        self.sent = []
        self.prepared = asyncio.Event()
        self.wrote = asyncio.Event()
        self.closed = False
        self.clear_count = 0
        self.close_count = 0

    async def prepare(self):
        self.prepared.set()

    async def read_audio_chunk(self):
        return await self.incoming.get()

    async def write_audio_chunk(self, chunk):
        if self.closed:
            raise ConnectionError("Socket already closed")
        self.sent.append(chunk)
        self.wrote.set()

    async def clear_output(self):
        self.clear_count += 1

    async def close(self):
        self.closed = True
        self.close_count += 1


class FakeSTT(BaseSTT):
    def __init__(self):
        self.chunks = []
        self.started = asyncio.Event()
        self.stopped = False
        self.final = None
        self.partial = None
        self.eou = None

    async def start_stream(self, on_partial=None, on_final=None, on_eou=None):
        self.partial, self.final, self.eou = on_partial, on_final, on_eou
        self.started.set()

    async def feed_audio(self, chunk):
        self.chunks.append(chunk)

    async def emit_final(self, text):
        if self.final:
            await self.final(text)
        if self.eou:
            await self.eou()

    async def stop_stream(self):
        self.stopped = True


class FakeTTS(BaseTTS):
    def __init__(self, audio=None):
        self.audio = pcm(2000, 40) if audio is None else audio
        self.texts = []
        self.closed = False

    async def synthesize(self, text):
        self.texts.append(text)
        return self.audio

    async def close(self):
        self.closed = True


class FakeConversationPort:
    def __init__(self, response="Test response"):
        self.response = response
        self.started = []
        self.utterances = []
        self.playback = []
        self.ended = []

    async def call_started(self, event):
        self.started.append(event)

    async def final_utterance(self, event):
        self.utterances.append(event)
        if self.response is None:
            return None
        return AuthorizedSpeech(request_id=f"fake-{event.sequence}", text=self.response)

    async def playback_fact(self, event):
        self.playback.append(event)

    async def call_ended(self, event):
        self.ended.append(event)


class FakeWriter:
    def __init__(self):
        self.frames = []
        self.closed = False
        self.fail_write = False

    def write(self, frame):
        if self.fail_write:
            raise ConnectionError("Disconnected")
        self.frames.append(frame)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


def socket_frame(kind, payload=b""):
    return struct.pack("!BH", kind, len(payload)) + payload
