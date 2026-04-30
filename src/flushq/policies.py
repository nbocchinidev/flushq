import asyncio
import time
import typing


T = typing.TypeVar("T")


class FlushPolicy(typing.Protocol[T]):
    """Protocol for controlling when FlushQueue flushes.

    Implement ``collect`` to define custom flush behavior."""

    async def collect(self, queue: asyncio.Queue[T], buffer: list[T]) -> None: ...


class IntervalPolicy(FlushPolicy[T]):
    """Flush when a record count or time limit is reached, whichever comes first.

    Args:
        max_wait_seconds: Max time to accumulate items before flushing.
        max_records: Max records to accumulate before flushing.
    """

    def __init__(self, max_wait_seconds: float, max_records: int) -> None:
        if max_wait_seconds <= 0:
            raise ValueError("max_wait_seconds must be positive")

        if max_records <= 0:
            raise ValueError("max_records must be positive")

        self.max_wait_seconds = max_wait_seconds
        self.max_records = max_records

    async def collect(self, queue: asyncio.Queue[T], buffer: list[T]) -> None:
        deadline = time.monotonic() + self.max_wait_seconds
        while len(buffer) < self.max_records:
            remaining = deadline - time.monotonic()

            try:
                item = await asyncio.wait_for(queue.get(), timeout=max(0, remaining))
                buffer.append(item)
            except asyncio.TimeoutError:
                break


class NaturalPolicy(FlushPolicy[T]):
    """Flush as soon as the previous flush completes, with whatever's queued.

    Batch size self-regulates: slow flush_fn means bigger batches, fast flush fn means smaller.
    No tuning required

    Args:
        max_records: Optional record cap to prevent unbounded batch sizes.
    """

    def __init__(self, max_records: int | None = None) -> None:
        if max_records is not None and max_records <= 0:
            raise ValueError("max_records must be positive")

        self.max_records = max_records

    async def collect(self, queue: asyncio.Queue[T], buffer: list[T]) -> None:
        item = await queue.get()
        buffer.append(item)
        limit = self.max_records or float("inf")

        while not queue.empty() and len(buffer) < limit:
            buffer.append(queue.get_nowait())
