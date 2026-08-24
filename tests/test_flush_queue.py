import asyncio
from collections import defaultdict
import contextlib
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


@pytest.mark.asyncio
async def test_flush_failure_unblocks_a_producer_on_a_full_queue():
    async def broken_flush(batch: list[int]) -> None:
        raise RuntimeError("sink is dead")

    q: FlushQueue[int] = FlushQueue(broken_flush, NaturalPolicy(), max_queue_size=2)

    async def produce() -> None:
        with pytest.raises(RuntimeError, match="sink is dead"):
            async with q:
                for n in range(10):
                    await q.enqueue(n)

    producer = asyncio.create_task(produce())
    _, pending = await asyncio.wait({producer}, timeout=1.0)
    if pending:
        producer.cancel()
        with contextlib.suppress(asyncio.CancelledError, RuntimeError):
            await producer
        pytest.fail("producer still blocked in enqueue() after flush task failed")
    await producer


@pytest.mark.asyncio
async def test_an_idle_queue_never_produces_a_flush():
    calls: list[list[int]] = []

    async def record(batch: list[int]) -> None:
        calls.append(batch)

    q: FlushQueue[int] = FlushQueue(
        record, IntervalPolicy(max_wait_seconds=0.05, max_records=10)
    )
    task = asyncio.create_task(q.run())
    await asyncio.sleep(0.2)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert calls == []


@pytest.mark.asyncio
async def test_a_batch_interrupted_mid_flush_is_not_redelivered():
    flushed: list[int] = []
    first_flush_started = asyncio.Event()

    async def slow_flush(batch: list[int]) -> None:
        first_flush_started.set()
        flushed.extend(batch)
        await asyncio.sleep(0.2)

    q: FlushQueue[int] = FlushQueue(slow_flush, NaturalPolicy())
    task = asyncio.create_task(q.run())
    await q.enqueue(1)
    await first_flush_started.wait()
    await q.enqueue(2)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert flushed == [1, 2]


@pytest.mark.asyncio
async def test_a_producer_that_never_awaits_is_drained_on_context_exit():
    flushed: list[int] = []

    async def sink(batch: list[int]) -> None:
        flushed.extend(batch)

    async with FlushQueue(sink, NaturalPolicy()) as q:
        for n in (1, 2, 3):
            await q.enqueue(n)

    assert flushed == [1, 2, 3]


@pytest.mark.asyncio
async def test_a_producer_that_never_awaits_is_drained_as_taskgroup_child():
    flushed: list[int] = []

    async def sink(batch: list[int]) -> None:
        flushed.extend(batch)

    q: FlushQueue[int] = FlushQueue(sink, NaturalPolicy())
    async with asyncio.TaskGroup() as tg:
        writer = tg.create_task(q.run())
        for n in (1, 2, 3):
            await q.enqueue(n)

        writer.cancel()

    assert flushed == [1, 2, 3]


@pytest.mark.asyncio
async def test_the_flush_policy_can_fire_while_producer_is_still_producing():
    calls: list[list[int]] = []

    async def record(batch: list[int]) -> None:
        calls.append(batch)

    async with FlushQueue(
        record, IntervalPolicy(max_wait_seconds=60, max_records=10)
    ) as q:
        for n in range(2000):
            await q.enqueue(n)

        flushes_during_production = len(calls)

    assert flushes_during_production >= 1
    assert sorted(n for batch in calls for n in batch) == list(range(2000))


@pytest.mark.asyncio
async def test_enqueue_never_reports_success_for_item_on_consumer_will_read():
    flushed: list[int] = []

    async def sink(batch: list[int]) -> None:
        flushed.extend(batch)

    q: FlushQueue[int] = FlushQueue(sink, NaturalPolicy())
    holder: dict[str, asyncio.Task[None]] = {}

    async def canceller() -> None:
        holder["writer"].cancel()

    c = asyncio.create_task(canceller())
    writer = asyncio.create_task(q.run())
    holder["writer"] = writer
    q._task = writer  # pyright: ignore[reportPrivateUsage] # what __aenter__ does
    accepted = True
    try:
        await q.enqueue(5)
    except RuntimeError:
        accepted = False

    await asyncio.sleep(0.01)
    for t in (writer, c):
        with contextlib.suppress(asyncio.CancelledError):
            await t

    assert flushed == [5] or not accepted, (
        "enqueue returned succcess but the item was lost: "
        f"flushed={flushed}, still queued={q._queue.qsize()}",  # pyright: ignore[reportPrivateUsage]
    )


@pytest.mark.asyncio
async def test_producer_whose_first_enqueue_races_run_start_is_not_stranded():
    async def broken_flush(batch: list[str]) -> None:
        await asyncio.sleep(0)  # let producers run
        raise RuntimeError("sink is dead")

    q: FlushQueue[str] = FlushQueue(broken_flush, NaturalPolicy(), max_queue_size=1)

    async def stale_producer() -> None:
        await q.enqueue("stale")

    async def fast_producer() -> None:
        with contextlib.suppress(RuntimeError):
            for i in range(10):
                await q.enqueue(f"fast-{i}")

    p_stale = asyncio.create_task(stale_producer())
    writer = asyncio.create_task(q.run())
    p_fast = asyncio.create_task(fast_producer())
    _, pending = await asyncio.wait({p_stale, p_fast}, timeout=0.01)
    stranded = p_stale in pending
    for t in (p_stale, p_fast, writer):
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError, RuntimeError):
            await t

    assert not stranded, "producer stranded in enqueue() after the flush task died"


@pytest.mark.asyncio
async def test_enqueue_raising_cancelled_means_item_not_delivered():
    flushed: list[int] = []

    async def sink(batch: list[int]) -> None:
        flushed.extend(batch)

    q: FlushQueue[int] = FlushQueue(sink, NaturalPolicy())
    writer = asyncio.create_task(q.run())
    await asyncio.sleep(0)  # writer tasks first step

    for n in range(512):
        await q.enqueue(n)

    async def producer() -> None:
        await q.enqueue(999)

    p = asyncio.create_task(producer())
    await asyncio.sleep(0)  # producer runs to streak yield now
    p.cancel()
    raised = False
    try:
        await p
    except asyncio.CancelledError:
        raised = True

    writer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await writer

    delivered = 999 in flushed
    assert not (raised and delivered), (
        "enqueue raised CancelledError (so item not enqueue) but item was delivered anyway"
    )


@pytest.mark.asyncio
async def test_full_queue_accept_path_refuses_when_consumer_dies():
    async def sink(batch: list[int]) -> None:
        raise RuntimeError("sink is dead")  # no await and dies in same step

    q: FlushQueue[int] = FlushQueue(sink, NaturalPolicy(), max_queue_size=1)
    writer = asyncio.create_task(q.run())
    await asyncio.sleep(0)
    await q.enqueue(1)
    accepted = True
    try:
        await q.enqueue(2)
    except RuntimeError:
        accepted = False

    writer.cancel()
    with contextlib.suppress(asyncio.CancelledError, RuntimeError):
        await writer

    assert not accepted, (
        f"accecpt path reported success for lost item that is still queued, queue={q._queue.qsize()}"  # pyright: ignore[reportPrivateUsage]
    )
