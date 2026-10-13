"""Exercise streaming, failure and reaping against real local subprocesses."""

import asyncio
import shutil
from contextlib import aclosing

import pytest

from tests.media.test_cleanup_regressions import executable
from voice_gateway.media.providers.decoder import decode_audio_stream


@pytest.fixture
def children(monkeypatch):
    processes = []
    create = asyncio.create_subprocess_exec

    async def track(*args, **kwargs):
        process = await create(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", track)
    yield processes
    assert processes and all(process.returncode is not None for process in processes)
    assert all(process.stdout.at_eof() and process.stderr.at_eof() for process in processes)


async def source(data=b"encoded fixture"):
    yield data


@pytest.mark.asyncio
async def test_decoder_yields_before_input_eof_and_closes_child(tmp_path, children):
    command = executable(
        tmp_path,
        "import sys,time\n"
        "sys.stdin.buffer.read(1)\n"
        "sys.stdout.buffer.write(b'\\x01\\0' * 160)\n"
        "sys.stdout.buffer.flush()\n"
        "time.sleep(30)",
    )
    input_closed = asyncio.Event()

    async def streaming_input():
        try:
            yield b"a"
            await asyncio.Event().wait()
        finally:
            input_closed.set()

    async with aclosing(decode_audio_stream(streaming_input(), executable=command)) as stream:
        assert await asyncio.wait_for(anext(stream), 2) == b"\x01\0" * 160
        assert not input_closed.is_set()
    assert input_closed.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", ["input", "output"])
async def test_decoder_enforces_limits_and_reaps_child(tmp_path, children, limit):
    command = executable(
        tmp_path,
        "import sys,time\n"
        "sys.stdout.buffer.write(b'\\0\\0' * 100000)\n"
        "sys.stdout.buffer.flush()\n"
        "time.sleep(30)",
    )
    kwargs = {"max_input_bytes": 1} if limit == "input" else {"max_output_bytes": 100}
    with pytest.raises(ValueError, match="limit"):
        async with aclosing(decode_audio_stream(source(), executable=command, **kwargs)) as stream:
            async for _ in stream:
                pass


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_decoder_timeout_or_cancellation_reaps_child(tmp_path, children, cancel):
    command = executable(tmp_path, "import time\ntime.sleep(30)")
    stream = decode_audio_stream(source(), executable=command, timeout=30 if cancel else 0.1)
    async with aclosing(stream):
        task = asyncio.create_task(anext(stream))
        if cancel:
            async with asyncio.timeout(2):
                while not children:
                    await asyncio.sleep(0.005)
            task.cancel("decoder test")
        with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
            await task


@pytest.mark.asyncio
async def test_decoder_source_failure_is_preserved_and_reaped(tmp_path, children):
    command = executable(tmp_path, "import time\ntime.sleep(30)")

    async def broken():
        yield b"a"
        raise ConnectionError("fixture stream failed")

    with pytest.raises(ConnectionError, match="fixture stream failed"):
        async with aclosing(decode_audio_stream(broken(), executable=command)) as stream:
            async for _ in stream:
                pass


@pytest.mark.asyncio
@pytest.mark.parametrize("output", [b"", b"\x01"])
async def test_decoder_rejects_empty_or_incomplete_pcm(tmp_path, children, output):
    command = executable(
        tmp_path,
        f"import sys\nsys.stdin.buffer.read()\nsys.stdout.buffer.write({output!r})",
    )
    with pytest.raises(ValueError, match="empty or incomplete"):
        async with aclosing(decode_audio_stream(source(), executable=command)) as stream:
            async for _ in stream:
                pass


@pytest.mark.asyncio
@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="optional local ffmpeg is unavailable")
async def test_real_ffmpeg_resamples_mp3_to_8khz_mono(children):
    encoder = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:sample_rate=24000:duration=0.2",
        "-ac",
        "2",
        "-f",
        "mp3",
        "pipe:1",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    encoded, _ = await asyncio.wait_for(encoder.communicate(), 5)
    assert encoder.returncode == 0

    async def network_chunks():
        for start in range(0, len(encoded), 113):
            yield encoded[start : start + 113]

    async with aclosing(decode_audio_stream(network_chunks(), timeout=5)) as stream:
        chunks = [chunk async for chunk in stream]
    assert all(len(chunk) % 2 == 0 for chunk in chunks)
    pcm = b"".join(chunks)
    # MP3 encoding adds bounded padding; 0.2 s at 8 kHz PCM16 requires 3200 bytes.
    assert 3200 <= len(pcm) < 6400 and any(pcm)
