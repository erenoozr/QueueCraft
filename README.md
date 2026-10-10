# QueueCraft

A multi-threaded task queue and scheduler written in pure Python 3.10+ (standard library only).

## Features

- **Priority queue** – thread-safe `heapq` + `threading.Condition`; higher priority (1–10) runs first, FIFO among equals.
- **Worker pool** – configurable number of worker threads, `ResultBackend` for results, `join()` and graceful `shutdown()`.
- **Retries with exponential backoff** – failed tasks are retried after `retry_delay * 2 ** retry_count` seconds.
- **Dead Letter Queue** – tasks exceeding `max_retries` (or raising a fatal `BaseException`) are moved to the DLQ for inspection.
- **Decorator API** – `@task(priority=5, max_retries=3)` adds `fn.delay(...)` and `fn.apply_in(seconds, ...)`.
- **Scheduled tasks** – `ScheduledTaskDispatcher` thread handles delayed tasks and retry backoff.
- **Live terminal dashboard** – busy/total workers, pending and scheduled tasks, success rate, retries, DLQ size.
- **Deadlock-safe by design** – no code path ever holds two locks at once.

## Usage

```python
from queuecraft import WorkerPool, task, set_default_pool

with WorkerPool(num_workers=4) as pool:
    set_default_pool(pool)

    @task(priority=8, max_retries=3, retry_delay=0.2)
    def add(a, b):
        return a + b

    t = add.delay(2, 3)          # run now
    add.apply_in(1.5, 10, 20)    # run in 1.5 seconds
    pool.join()
    print(t.state, t.result)     # TaskState.SUCCESS 5
```

## Stress test

```bash
python queuecraft.py
```

Fires 20 tasks with random priorities; some fail temporarily (recovered by retries), some fail permanently (end up in the DLQ). The run asserts that every task finishes, the DLQ count matches expectations and all threads shut down cleanly.

## Requirements

Python 3.10+ — no third-party dependencies.
