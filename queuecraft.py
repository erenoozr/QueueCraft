"""QueueCraft: multi-threaded task queue and scheduler (standard library only).

Lock discipline (deadlock protection): no code path ever holds two locks at once;
calls into user functions and other components are always made outside of any lock.
"""
from __future__ import annotations

import heapq
import itertools
import logging
import random
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

log = logging.getLogger("queuecraft")


# ───────────────────────────── STAGE 1: Model & queue ─────────────────────────────

class TaskState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


@dataclass(eq=False)
class Task:
    name: str
    fn: Callable[..., Any]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: int = 5
    max_retries: int = 3
    retry_delay: float = 0.1
    state: TaskState = TaskState.PENDING
    task_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    retry_count: int = 0
    result: Any = None
    error: Optional[BaseException] = None

    def __post_init__(self) -> None:
        if not 1 <= self.priority <= 10:
            raise ValueError(f"priority must be between 1 and 10, got: {self.priority}")
        if self.max_retries < 0 or self.retry_delay < 0:
            raise ValueError("max_retries and retry_delay must not be negative")


class PriorityTaskQueue:
    """Higher priority first; FIFO among equal priorities (thanks to a sequence counter)."""

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, Task]] = []
        self._seq = itertools.count()
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)

    def put(self, task: Task) -> None:
        with self._not_empty:
            heapq.heappush(self._heap, (-task.priority, next(self._seq), task))
            self._not_empty.notify()

    def get(self, timeout: Optional[float] = None) -> Optional[Task]:
        """Returns None after timeout if no task is available (so workers can be stopped)."""
        with self._not_empty:
            if not self._not_empty.wait_for(lambda: self._heap, timeout):
                return None
            return heapq.heappop(self._heap)[2]

    def __len__(self) -> int:
        with self._lock:
            return len(self._heap)


# ───────────────────────────── STAGE 2: Result backend & workers ─────────────────────────────

class ResultBackend:
    def __init__(self) -> None:
        self._results: dict[str, Any] = {}
        self._lock = threading.Lock()

    def store(self, task_id: str, result: Any) -> None:
        with self._lock:
            self._results[task_id] = result

    def get(self, task_id: str, default: Any = None) -> Any:
        with self._lock:
            return self._results.get(task_id, default)

    def __len__(self) -> int:
        with self._lock:
            return len(self._results)


# ───────────────────────────── STAGE 3: DLQ ─────────────────────────────

class DeadLetterQueue:
    def __init__(self) -> None:
        self._tasks: list[Task] = []
        self._lock = threading.Lock()

    def add(self, task: Task) -> None:
        with self._lock:
            self._tasks.append(task)

    def snapshot(self) -> list[Task]:
        with self._lock:
            return list(self._tasks)

    def __len__(self) -> int:
        with self._lock:
            return len(self._tasks)


# ───────────────────────────── STAGE 4: Scheduler ─────────────────────────────

class ScheduledTaskDispatcher(threading.Thread):
    """Moves due tasks into the main queue. Used for both delayed tasks and retry backoff."""

    def __init__(self, target: PriorityTaskQueue) -> None:
        super().__init__(name="QC-Scheduler", daemon=True)
        self._target = target
        self._heap: list[tuple[float, int, Task]] = []
        self._seq = itertools.count()
        self._cond = threading.Condition(threading.Lock())
        self._stopping = False

    def schedule(self, task: Task, delay: float) -> None:
        with self._cond:
            heapq.heappush(self._heap, (time.monotonic() + max(0.0, delay), next(self._seq), task))
            self._cond.notify()

    def __len__(self) -> int:
        with self._cond:
            return len(self._heap)

    def stop(self) -> None:
        with self._cond:
            self._stopping = True
            self._cond.notify()

    def run(self) -> None:
        while True:
            due: list[Task] = []
            with self._cond:
                if self._stopping:
                    return
                now = time.monotonic()
                while self._heap and self._heap[0][0] <= now:
                    due.append(heapq.heappop(self._heap)[2])
                if not due:
                    timeout = self._heap[0][0] - now if self._heap else None
                    self._cond.wait(timeout)
                    continue
            for t in due:  # enqueue outside our own lock: no nested locking
                self._target.put(t)


# ───────────────────────────── Worker & pool ─────────────────────────────

class WorkerThread(threading.Thread):
    def __init__(self, pool: "WorkerPool", index: int) -> None:
        super().__init__(name=f"QC-Worker-{index}", daemon=True)
        self._pool = pool
        self.busy = False

    def run(self) -> None:
        pool = self._pool
        while not pool._stop.is_set():
            task = pool.queue.get(timeout=0.1)
            if task is None:
                continue
            self.busy = True
            try:
                pool._execute(task)
            finally:
                self.busy = False


class WorkerPool:
    def __init__(self, num_workers: int = 4) -> None:
        if num_workers < 1:
            raise ValueError("num_workers must be >= 1")
        self.queue = PriorityTaskQueue()
        self.results = ResultBackend()
        self.dlq = DeadLetterQueue()
        self.scheduler = ScheduledTaskDispatcher(self.queue)
        self._stop = threading.Event()
        self._workers = [WorkerThread(self, i) for i in range(num_workers)]
        # Counters + number of unfinished tasks under a single lock
        self._stats_cond = threading.Condition(threading.Lock())
        self._outstanding = 0
        self.succeeded = 0
        self.failed = 0
        self.retries = 0
        self._started = False

    # lifecycle
    def start(self) -> "WorkerPool":
        if not self._started:
            self._started = True
            self.scheduler.start()
            for w in self._workers:
                w.start()
        return self

    def __enter__(self) -> "WorkerPool":
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.shutdown(wait=True)

    def submit(self, task: Task, delay: float = 0.0) -> Task:
        if self._stop.is_set():
            raise RuntimeError("pool is shut down, no new tasks accepted")
        with self._stats_cond:
            self._outstanding += 1
        if delay > 0:
            self.scheduler.schedule(task, delay)
        else:
            self.queue.put(task)
        return task

    def join(self, timeout: Optional[float] = None) -> bool:
        """Waits until every task is SUCCESS or in the DLQ (retries included)."""
        with self._stats_cond:
            return self._stats_cond.wait_for(lambda: self._outstanding == 0, timeout)

    def shutdown(self, wait: bool = True, timeout: Optional[float] = None) -> bool:
        """wait=True: finishes remaining work first (graceful). Returns whether all work finished."""
        drained = self.join(timeout) if wait else False
        self._stop.set()
        self.scheduler.stop()
        for w in self._workers:
            w.join(timeout=5)
        self.scheduler.join(timeout=5)
        return drained

    # execution
    def _execute(self, task: Task) -> None:
        task.state = TaskState.RUNNING
        try:
            result = task.fn(*task.args, **task.kwargs)
        except Exception as exc:  # a user error must not kill the worker
            task.error = exc
            if task.retry_count < task.max_retries:
                backoff = task.retry_delay * (2 ** task.retry_count)
                task.retry_count += 1
                task.state = TaskState.RETRYING
                log.warning("%s failed: %r -> retry #%d in %.2fs", task.name, exc, task.retry_count, backoff)
                with self._stats_cond:
                    self.retries += 1
                self.scheduler.schedule(task, backoff)
                return
            task.state = TaskState.FAILED
            log.error("%s failed permanently, moved to DLQ: %r", task.name, exc)
            self.dlq.add(task)
            self._finish(ok=False)
            return
        except BaseException as exc:  # SystemExit etc.: no retry, but join() must never hang
            task.error, task.state = exc, TaskState.FAILED
            log.critical("%s fatal error, moved to DLQ: %r", task.name, exc)
            self.dlq.add(task)
            self._finish(ok=False)
            return
        task.result, task.state = result, TaskState.SUCCESS
        self.results.store(task.task_id, result)
        self._finish(ok=True)

    def _finish(self, ok: bool) -> None:
        with self._stats_cond:
            if ok:
                self.succeeded += 1
            else:
                self.failed += 1
            self._outstanding -= 1
            self._stats_cond.notify_all()

    # metrics
    def metrics(self) -> dict[str, Any]:
        with self._stats_cond:
            ok, bad, retries, outstanding = self.succeeded, self.failed, self.retries, self._outstanding
        done = ok + bad
        return {
            "workers": sum(w.is_alive() for w in self._workers),
            "busy": sum(w.busy for w in self._workers),
            "pending": len(self.queue),
            "scheduled": len(self.scheduler),
            "outstanding": outstanding,
            "success": ok,
            "failed": bad,
            "retries": retries,
            "success_rate": (ok / done * 100) if done else 0.0,
            "dlq": len(self.dlq),
        }


# ───────────────────────────── STAGE 4: Decorator ─────────────────────────────

_default_pool: Optional[WorkerPool] = None


def set_default_pool(pool: Optional[WorkerPool]) -> None:
    global _default_pool
    _default_pool = pool


def task(priority: int = 5, max_retries: int = 3, retry_delay: float = 0.1,
         pool: Optional[WorkerPool] = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """@task(...) -> fn.delay(*a, **kw) enqueues now, fn.apply_in(sec, *a, **kw) after sec seconds."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        def _submit(delay: float, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Task:
            target = pool or _default_pool
            if target is None:
                raise RuntimeError("no pool: pass pool= or call set_default_pool()")
            t = Task(name=fn.__name__, fn=fn, args=args, kwargs=kwargs,
                     priority=priority, max_retries=max_retries, retry_delay=retry_delay)
            return target.submit(t, delay=delay)

        fn.delay = lambda *a, **kw: _submit(0.0, a, kw)  # type: ignore[attr-defined]
        fn.apply_in = lambda seconds, *a, **kw: _submit(seconds, a, kw)  # type: ignore[attr-defined]
        return fn

    return decorator


# ───────────────────────────── STAGE 5: Dashboard ─────────────────────────────

class MetricsReporter(threading.Thread):
    def __init__(self, pool: WorkerPool, interval: float = 0.25) -> None:
        super().__init__(name="QC-Dashboard", daemon=True)
        self._pool, self._interval = pool, interval
        self._stop_evt = threading.Event()

    @staticmethod
    def render(m: dict[str, Any]) -> str:
        return (f"Workers {m['busy']}/{m['workers']} | Pending {m['pending']:>2} | Scheduled {m['scheduled']:>2} | "
                f"Succeeded {m['success']:>2} | Retries {m['retries']:>2} | DLQ {m['dlq']:>2} | "
                f"Success {m['success_rate']:5.1f}%")

    def run(self) -> None:
        while not self._stop_evt.wait(self._interval):
            sys.stdout.write("\r" + self.render(self._pool.metrics()))
            sys.stdout.flush()

    def stop(self) -> None:
        self._stop_evt.set()
        self.join()
        sys.stdout.write("\r" + self.render(self._pool.metrics()) + "\n")


# ───────────────────────────── Stress test & self-check ─────────────────────────────

def _self_check() -> None:
    q = PriorityTaskQueue()
    for p in (3, 10, 1, 7, 10):
        q.put(Task(name=f"p{p}", fn=lambda: None, priority=p))
    order = [q.get(0).priority for _ in range(5)]  # type: ignore[union-attr]
    assert order == [10, 10, 7, 3, 1], order
    assert q.get(timeout=0.01) is None
    try:
        Task(name="x", fn=lambda: None, priority=11)
        raise AssertionError("invalid priority was accepted")
    except ValueError:
        pass
    with WorkerPool(num_workers=1) as pool:  # a BaseException must not kill the worker or hang join
        def boom() -> None:
            raise SystemExit("fatal")
        pool.submit(Task(name="boom", fn=boom))
        ok = pool.submit(Task(name="ok", fn=lambda: 42))
        assert pool.join(timeout=5) and len(pool.dlq) == 1 and ok.result == 42


def stress_test(n_tasks: int = 20, workers: int = 4) -> None:
    random.seed()
    attempts: dict[int, int] = {}
    attempts_lock = threading.Lock()

    with WorkerPool(num_workers=workers) as pool:
        set_default_pool(pool)

        def job(i: int, fail_times: int) -> int:
            with attempts_lock:
                attempts[i] = attempts.get(i, 0) + 1
                n = attempts[i]
            time.sleep(random.uniform(0.05, 0.25))
            if n <= fail_times:
                raise RuntimeError(f"task {i} intentional failure (attempt {n})")
            return i * i

        reporter = MetricsReporter(pool)
        reporter.start()
        # random priority per task; ~30% transient failures (fixed by retry), ~15% permanent (DLQ)
        expected_dlq = 0
        for i in range(n_tasks):
            r = random.random()
            fail_times = 0 if r < 0.55 else (random.randint(1, 2) if r < 0.85 else 99)
            expected_dlq += fail_times > 2
            decorated = task(priority=random.randint(1, 10), max_retries=2, retry_delay=0.1)(job)
            if i % 5 == 0:
                decorated.apply_in(0.5, i, fail_times)  # some are delayed
            else:
                decorated.delay(i, fail_times)

        finished = pool.join(timeout=60)
        reporter.stop()
        assert finished, "tasks did not finish within 60s (deadlock?)"
        m = pool.metrics()
        assert m["success"] + m["failed"] == n_tasks, m
        assert m["dlq"] == expected_dlq, (m, expected_dlq)
        for t in pool.dlq.snapshot():
            assert t.state is TaskState.FAILED and t.retry_count == t.max_retries
            print(f"  DLQ: {t.name} id={t.task_id[:8]} priority={t.priority} error={t.error!r}")

    set_default_pool(None)
    alive = [th.name for th in threading.enumerate() if th.name.startswith("QC-")]
    assert not alive, f"threads still running: {alive}"
    print(f"Stress test passed: {m['success']} succeeded, {m['failed']} in DLQ, "
          f"{m['retries']} retries, clean shutdown.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.CRITICAL)  # keep logs from breaking the dashboard line; set WARNING for details
    _self_check()
    stress_test()
