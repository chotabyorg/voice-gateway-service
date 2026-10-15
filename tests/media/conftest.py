"""Observe actual child process and pipe termination on Windows and POSIX."""

import asyncio

import pytest


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
