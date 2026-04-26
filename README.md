A lightweight async batching queue for Python. Collect items and flush them in batches, by record count and time interval, or automatically as fast as your sink can consume. Zero dependencies. Cancel safe.

Flush policies: `IntervalPolicy` (flush at N records or M seconds) and `NaturalPolicy` (flush as soon as the previous flush completes)

## Install
```bash
pip install flushq
```

## Usage

```python
import asyncio
from flushq import FlushQueue, IntervalPolicy

async def save_to_db(events: list[Event]):
    await db.bulk_insert(events)

policy = IntervalPolicy(max_wait_seconds=2.0, max_records=500)

async with FlushQueue(flush_fn=save_to_db, policy=policy) as q:
    async for event in event_stream():
        await q.enqueue(event)
```

Flushes to `save_to_db` whenever 500 events accumulate or 2 seconds pass, whichever comes first.
Backpressure is handled automatically — `enqueue` blocks if the internal buffer is full.

## NaturalPolicy — no tuning required

```python
from flushq import FlushQueue, NaturalPolicy

async def send_to_api(events: list[Event]):
    await api.bulk_send(events)

policy = NaturalPolicy()

async with FlushQueue(flush_fn=send_to_api, policy=policy) as q:
    async for event in event_stream():
        await q.enqueue(event)
```

Flushes as soon as the previous flush completes, with whatever has accumulated in the meantime.
When `send_to_api` is slow, batches grow larger. When it's fast, batches stay small.
No `max_wait_seconds` or `max_records` to tune — throughput self-regulates to match your sink.

If you want a ceiling on batch size:

```python
policy = NaturalPolicy(max_records=1000)
```

## Deduplication

Pass `dedupe_key` to drop duplicate items within each batch. The first occurrence is kept.

```python
async with FlushQueue(
    flush_fn=save_to_db,
    policy=policy,
    dedupe_key=lambda e: e.id,
) as q:
    async for event in event_stream():
        await q.enqueue(event)
```

If two events with the same `id` land in the same flush window, only the first is passed to `save_to_db`.
Deduplication is scoped per batch — the same key can appear across separate flushes.