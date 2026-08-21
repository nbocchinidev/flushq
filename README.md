A lightweight async batching queue for Python. Collect items and flush them in batches, by record count and time interval, or automatically as fast as your sink can consume. Zero dependencies. Cancel safe.

Flush policies: `IntervalPolicy` (flush at N records or when the oldest buffered item turns M seconds old) and `NaturalPolicy` (flush as soon as the previous flush completes)

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

Flushes to `save_to_db` whenever 500 events accumulate or the oldest buffered event has waited 2 seconds, whichever comes first. An idle queue never flushes.
Backpressure is handled automatically — `enqueue` blocks if the internal buffer is full.

## Running it

The context manager above is fine for a single producer. When the queue is one of several concurrent tasks, run it as a TaskGroup child instead. That way a failed flush then cancels the whole group immediately, instead of waiting until the next enqueue to raise.

```python
async with asyncio.TaskGroup() as tg:
    q = FlushQueue(flush_fn=save_to_db, policy=policy)
    writer = tg.create_task(q.run())
    
    async for event in event_stream():
        await q.enqueue(event)
    
    writer.cancel() # end of stream, buffered events are drained and flushed
```

In both usage options, a dead flush task never strands a producer because enqueue raises with the flush's error instead of blocking on a queue nothing will drain

## Delivery contract

- A batch reaches flush_fn at most once, and is never empty.
- A flush interrupted by cancellation is not retried, flush_fn owns the atomicity of its own side effects.
- On shutdown, buffered items are drained and flushed, bounded by ``max_shutdown_wait``

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