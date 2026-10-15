"""Bounded streaming ffmpeg decoder. No audio files, shell, or retained stderr."""

import asyncio

from ..completion import bounded_call, deadline_timeout, finish_owned_task


async def decode_audio_stream(
    chunks,
    *,
    executable="ffmpeg",
    max_input_bytes=3_000_000,
    max_output_bytes=1_920_000,
    timeout=120,
):
    if timeout <= 0 or max_input_bytes < 1 or max_output_bytes < 2:
        raise ValueError("Invalid decoder bounds")
    deadline = asyncio.get_running_loop().time() + timeout
    command = [executable] if isinstance(executable, str) else list(executable)
    process = await asyncio.create_subprocess_exec(
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
        limit=65536,
    )

    def kill():
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass

    async def feed():
        received = 0
        try:
            async for chunk in chunks:
                received += len(chunk)
                if received > max_input_bytes:
                    raise ValueError("Encoded audio exceeds configured limit")
                process.stdin.write(chunk)
                await process.stdin.drain()
        except BaseException:
            kill()
            raise
        finally:
            process.stdin.close()

    async def drain_stderr():
        while await process.stderr.read(65536):
            pass

    feeder = asyncio.create_task(feed(), name="voice-decoder-input")
    stderr = asyncio.create_task(drain_stderr(), name="voice-decoder-stderr")
    received, carry = 0, b""
    try:
        while True:
            async with deadline_timeout(deadline):
                chunk = await process.stdout.read(3200)
            if not chunk:
                break
            received += len(chunk)
            if received > max_output_bytes:
                raise ValueError("Decoded audio exceeds configured limit")
            data = carry + chunk
            end = len(data) // 2 * 2
            carry = data[end:]
            if end:
                yield data[:end]
        async with deadline_timeout(deadline):
            await feeder
            await stderr
            code = await process.wait()
            if code:
                raise RuntimeError("Audio decoder failed")
            if carry or not received:
                raise ValueError("Decoder returned empty or incomplete PCM")
    finally:
        kill()
        feeder.cancel()

        async def reap():
            # A killed child can still leave a full stdout pipe. Drain it so
            # asyncio can close all pipe transports before the loop exits.
            async def discard_output():
                while await process.stdout.read(65536):
                    pass

            await asyncio.gather(
                feeder, stderr, discard_output(), process.wait(), return_exceptions=True
            )

        cleanup = asyncio.create_task(bounded_call(reap(), timeout=2), name="voice-decoder-reap")
        await finish_owned_task(cleanup)
