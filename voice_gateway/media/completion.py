"""Finish an owned cleanup task before propagating caller cancellation."""

import asyncio


def observe_task(task):
    if not task.cancelled():
        task.exception()


async def bounded_call(awaitable, *, timeout, cancellation_grace=0.1):
    """Run cleanup without letting cancellation-resistant code hold shutdown forever."""
    task = asyncio.create_task(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    except asyncio.CancelledError:
        task.cancel("cleanup owner cancelled")
        done, pending = await asyncio.wait({task}, timeout=cancellation_grace)
        if pending:
            task.add_done_callback(observe_task)
        else:
            observe_task(task)
        raise
    if not done:
        task.cancel("cleanup deadline")
        done, pending = await asyncio.wait({task}, timeout=cancellation_grace)
        if pending:
            task.add_done_callback(observe_task)
        else:
            observe_task(task)
        return False
    task.result()
    return True


async def finish_owned_task(task, *, cancellation=None):
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if asyncio.current_task().cancelling():
                cancellation = cancellation or exc
            if task.done():
                break
        except Exception:
            break  # task.result() below preserves the failure unless caller cancelled.
    if cancellation is not None:
        if not task.cancelled():
            task.exception()  # Observe cleanup failure even when cancellation wins.
        raise cancellation
    return task.result()
