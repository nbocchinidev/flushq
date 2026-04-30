import asyncio
import time
import types
import typing


from flushq.policies import FlushPolicy


T = typing.TypeVar("T")


class FlushQueue(typing.Generic[T]):
    """Async buffer that collects items and flushes them in batches.

    Items are enqueued with ``enqueue`` and batched according to the provided ``FlushPolicy``. Each
    batch is passed to ``flush_fn``. Deduplicate within each batch by providing ``dedupe key``.

    Use as an async context manager to handle startup and clean shutdown:

        async with FlushQueue(flush_fn, policy) as q:
            await q.enqueue(item)

    Args:
        flush_fn: Async callable that receives each batch.
        policy: Controls when to flush (IntervalPolicy or NaturalPolicy).
        dedupe_key: Optional callable returning a hashable key per item.
            Deduplication is scoped per batch so the same key may appear across separate flushes.
        max_queue_size: Backpressure limit for the internal buffer.
        max_shutdown_wait: Max seconds to wait for interal queue to be drained on shutdown
        max_shutdown_batch_size: Max items to buffer in memory before flushing on shutdown
    """

    def __init__(
        self,
        flush_fn: typing.Callable[[list[T]], typing.Awaitable[None]],
        policy: FlushPolicy[T],
        *,
        dedupe_key: typing.Callable[[T], typing.Hashable] | None = None,
        max_queue_size: int = 10_000,
        max_shutdown_wait: float | None = None,
        max_shutdown_batch_size: int = 10_000,
    ) -> None:
        self._flush_fn = flush_fn
        self._policy = policy
        self._dedupe_key = dedupe_key
        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=max_queue_size)
        self._task: asyncio.Task[None] | None = None
        self._max_shutdown_wait = max_shutdown_wait
        self._max_shutdown_batch_size = max_shutdown_batch_size

    async def enqueue(self, item: T) -> None:
        await self._queue.put(item)

    async def run(self) -> None:
        batch: list[T] = []

        try:
            while True:
                await self._policy.collect(self._queue, batch)
                await self._flush(batch)
                batch = []
        except asyncio.CancelledError:
            deadline = time.monotonic() + (self._max_shutdown_wait or float("inf"))

            while not self._queue.empty() and time.monotonic() < deadline:
                batch.append(self._queue.get_nowait())
                if len(batch) == self._max_shutdown_batch_size:
                    try:
                        await asyncio.wait_for(
                            self._flush(batch), deadline - time.monotonic()
                        )
                        batch = []
                    except asyncio.TimeoutError:
                        return
            try:
                await asyncio.wait_for(self._flush(batch), deadline - time.monotonic())
            except asyncio.TimeoutError:
                return

    async def _flush(self, batch: list[T]) -> None:
        if self._dedupe_key is not None:
            seen: set[typing.Hashable] = set()
            dedup_batch: list[T] = []

            for item in batch:
                key = self._dedupe_key(item)

                if key in seen:
                    continue

                seen.add(key)
                dedup_batch.append(item)

            batch = dedup_batch

        await self._flush_fn(batch)

    async def __aenter__(self) -> typing.Self:
        if self._task is not None:
            raise RuntimeError("running task has already been started")

        self._task = asyncio.create_task(self.run())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        if self._task is None:
            raise RuntimeError("no running task")

        self._task.cancel()

        try:
            await self._task
        except asyncio.CancelledError:
            pass
