"""Test helpers shared across letterbox test modules.

`wait_for` is the load-bearing helper: every async-arrival test from
Phase 4 onward needs to poll for "did the notification show up yet?"
rather than `asyncio.sleep`ing for a fixed delay. Fixed sleeps pass
locally and fail on loaded CI; polling adapts to wall-clock jitter.
"""
from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, Union

Predicate = Callable[[], Union[bool, Awaitable[bool]]]


async def wait_for(
    predicate: Predicate,
    timeout: float = 5.0,
    interval: float = 0.05,
) -> None:
    """Poll ``predicate`` until it returns truthy or ``timeout`` elapses.

    The predicate may be sync (returns bool) or async (returns Awaitable[bool]).

    Args:
        predicate: Callable invoked each poll iteration. Truthy return ends the wait.
        timeout: Maximum total seconds to wait before raising.
        interval: Seconds to sleep between polls. Default 50ms is a balance
            between responsiveness and CPU burn.

    Raises:
        TimeoutError: If ``timeout`` elapses without the predicate returning truthy.
            The message includes the elapsed wall-clock time so flake diagnoses
            can distinguish "barely missed" from "never going to happen".
    """
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        if time.monotonic() >= deadline:
            elapsed = time.monotonic() - (deadline - timeout)
            raise TimeoutError(
                f"wait_for: predicate did not become truthy within {timeout}s "
                f"(elapsed {elapsed:.3f}s)"
            )
        await asyncio.sleep(interval)
