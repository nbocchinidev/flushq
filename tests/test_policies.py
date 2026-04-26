import asyncio
import time
import pytest

from flushq.policies import IntervalPolicy, NaturalPolicy


class TestIntervalPolicy:
    @pytest.mark.asyncio
    async def test_max_records_triggers_collect(self):
        q: asyncio.Queue[int] = asyncio.Queue(maxsize=4)
        policy = IntervalPolicy[int](max_wait_seconds=10.0, max_records=4)

        for i in range(4):
            q.put_nowait(i)

        start = time.monotonic()
        batch: list[int] = []
        await policy.collect(q, batch)
        end = time.monotonic()

        assert batch == [0, 1, 2, 3]
        assert 10 > end - start

    @pytest.mark.asyncio
    async def test_max_wait_triggers_collect(self):
        q: asyncio.Queue[int] = asyncio.Queue(maxsize=2)
        policy = IntervalPolicy[int](max_wait_seconds=0.01, max_records=10)

        for i in range(2):
            q.put_nowait(i)

        start = time.monotonic()
        batch: list[int] = []
        await policy.collect(q, batch)
        end = time.monotonic()

        assert batch == [0, 1]
        assert 0.01 <= end - start < 0.02

    def test_negative_max_wait_raises(self):
        with pytest.raises(ValueError):
            IntervalPolicy[int](max_wait_seconds=-1, max_records=10)

    def test_negative_max_records_raises(self):
        with pytest.raises(ValueError):
            IntervalPolicy[int](max_wait_seconds=10, max_records=-1)


class TestNaturalPolicy:
    @pytest.mark.asyncio
    async def test_max_records_triggers_collect(self):
        q: asyncio.Queue[int] = asyncio.Queue(maxsize=10)
        policy = NaturalPolicy[int](max_records=6)

        for i in range(10):
            q.put_nowait(i)

        batch: list[int] = []
        await policy.collect(q, batch)
        assert batch == [0, 1, 2, 3, 4, 5]
        batch: list[int] = []
        await policy.collect(q, batch)
        assert batch == [6, 7, 8, 9]

    @pytest.mark.asyncio
    async def test_empty_queue_triggers_collect(self):
        q: asyncio.Queue[int] = asyncio.Queue(maxsize=10)
        policy = NaturalPolicy[int](max_records=6)

        for i in range(4):
            q.put_nowait(i)

        batch: list[int] = []
        await policy.collect(q, batch)
        assert batch == [0, 1, 2, 3]

    @pytest.mark.asyncio
    async def test_no_max_records_drains_available(self):
        q: asyncio.Queue[int] = asyncio.Queue(maxsize=10)
        policy = NaturalPolicy[int]()

        for i in range(10):
            q.put_nowait(i)

        batch: list[int] = []
        await policy.collect(q, batch)
        assert batch == list(range(10))

    @pytest.mark.asyncio
    async def test_blocks_until_item_available(self):
        q: asyncio.Queue[int] = asyncio.Queue()
        policy = NaturalPolicy[int]()

        async def delayed_put():
            await asyncio.sleep(0.05)
            q.put_nowait(42)

        asyncio.create_task(delayed_put())
        batch: list[int] = []
        await policy.collect(q, batch)
        assert batch == [42]

    @pytest.mark.asyncio
    async def test_one_max_record(self):
        q: asyncio.Queue[int] = asyncio.Queue(maxsize=10)
        policy = NaturalPolicy[int](max_records=1)
        q.put_nowait(1)
        batch: list[int] = []
        await policy.collect(q, batch)
        assert batch == [1]

    def test_negative_max_records_raises(self):
        with pytest.raises(ValueError):
            NaturalPolicy[int](max_records=-1)
