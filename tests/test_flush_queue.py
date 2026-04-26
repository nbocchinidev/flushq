import asyncio
from collections import defaultdict
import typing
import pytest

from flushq import FlushQueue, NaturalPolicy, IntervalPolicy, FlushPolicy


@pytest.mark.asyncio
async def test_lifecycle():
    db: dict[str, int] = defaultdict(int)
    policy = NaturalPolicy[str](max_records=5)
    text = "abcdefabcdabcaba"

    async def flush_fn(items: list[str]):
        for item in items:
            db[item] += 1

    async with FlushQueue[str](flush_fn=flush_fn, policy=policy) as fq:
        for c in text:
            await fq.enqueue(c)
            await asyncio.sleep(0)

    want = {"a": 5, "b": 4, "c": 3, "d": 2, "e": 1, "f": 1}
    assert want == db


@pytest.mark.asyncio
async def test_cancellation():
    db: dict[str, int] = defaultdict(int)
    policy = IntervalPolicy[str](max_records=1000, max_wait_seconds=5)
    text = "abcdefabcdabcaba"

    async def flush_fn(items: list[str]):
        for item in items:
            db[item] += 1

    async with FlushQueue[str](flush_fn=flush_fn, policy=policy) as fq:
        for c in text:
            await fq.enqueue(c)
            await asyncio.sleep(0)

    want = {"a": 5, "b": 4, "c": 3, "d": 2, "e": 1, "f": 1}
    assert want == db


T = typing.TypeVar("T")


class BlockingPolicy(FlushPolicy[T]):
    async def collect(self, queue: asyncio.Queue[T], buffer: list[T]) -> None:
        await asyncio.sleep(float("inf"))  # block until cancelled


@pytest.mark.asyncio
async def test_shutdown_mid_batch_flush():
    flushed_batches: list[list[str]] = []

    async def flush_fn(items: list[str]):
        flushed_batches.append(list(items))

    policy = BlockingPolicy[str]()
    items = list("abcdef")
    async with FlushQueue[str](
        flush_fn=flush_fn,
        policy=policy,
        max_shutdown_batch_size=3,
    ) as fq:
        for item in items:
            await fq.enqueue(item)
            await asyncio.sleep(0)

    all_flushed = [item for batch in flushed_batches for item in batch]
    assert sorted(all_flushed) == sorted(items)
    assert any(len(b) == 3 for b in flushed_batches)


@pytest.mark.asyncio
async def test_shutdown_mid_batch_flush_timeout():
    flushed: list[str] = []

    async def slow_flush_fn(items: list[str]):
        await asyncio.sleep(10)
        flushed.extend(items)

    policy = BlockingPolicy[str]()
    items = list("abcdef")

    async with FlushQueue[str](
        flush_fn=slow_flush_fn,
        policy=policy,
        max_shutdown_batch_size=3,
        max_shutdown_wait=0.01,
    ) as fq:
        for item in items:
            await fq.enqueue(item)
            await asyncio.sleep(0)

    assert flushed == []


@pytest.mark.asyncio
async def test_shutdown_final_flush_timeout():
    flushed: list[str] = []

    async def slow_flush_fn(items: list[str]):
        await asyncio.sleep(10)
        flushed.extend(items)

    policy = BlockingPolicy[str]()
    items = list("ab")

    async with FlushQueue[str](
        flush_fn=slow_flush_fn,
        policy=policy,
        max_shutdown_batch_size=3,
        max_shutdown_wait=0.01,
    ) as fq:
        for item in items:
            await fq.enqueue(item)
            await asyncio.sleep(0)

    assert flushed == []


@pytest.mark.asyncio
async def test_double_enter_raises():
    async def flush_fn(items: list[T]):
        pass  # type: ignore

    fq = FlushQueue[int](flush_fn=flush_fn, policy=NaturalPolicy())

    async with fq:
        with pytest.raises(RuntimeError, match="already been started"):
            await fq.__aenter__()


@pytest.mark.asyncio
async def test_exit_without_enter_raises():
    async def flush_fn(items: list[T]):
        pass  # type: ignore

    fq = FlushQueue[int](flush_fn=flush_fn, policy=NaturalPolicy())

    with pytest.raises(RuntimeError, match="no running task"):
        await fq.__aexit__(None, None, None)  # type: ignore


@pytest.mark.asyncio
async def test_batch_with_dedup():
    flushed: list[tuple[str, int]] = []
    policy = NaturalPolicy[tuple[str, int]](max_records=50)

    async def flush_fn(items: list[tuple[str, int]]):
        flushed.extend(items)

    async with FlushQueue[tuple[str, int]](
        flush_fn=flush_fn,
        policy=policy,
        dedupe_key=lambda x: x[0],
    ) as fq:
        await fq.enqueue(("a", 1))
        await fq.enqueue(("a", 2))
        await fq.enqueue(("b", 1))
        await asyncio.sleep(0)

    assert flushed == [("a", 1), ("b", 1)]


@pytest.mark.asyncio
async def test_dedup_preserves_first_occurrence():
    flushed: list[tuple[str, int]] = []

    async def flush_fn(items: list[tuple[str, int]]):
        flushed.extend(items)

    async with FlushQueue[tuple[str, int]](
        flush_fn=flush_fn,
        policy=NaturalPolicy(max_records=50),
        dedupe_key=lambda x: x[0],
    ) as fq:
        await fq.enqueue(("a", 1))
        await fq.enqueue(("b", 1))
        await fq.enqueue(("a", 2))
        await asyncio.sleep(0)

    assert flushed == [("a", 1), ("b", 1)]
