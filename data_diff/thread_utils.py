import itertools
from queue import PriorityQueue
from collections import deque
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.thread import _WorkItem
from time import sleep
from typing import Callable, Iterator, Optional


class AutoPriorityQueue(PriorityQueue):
    """Overrides PriorityQueue to automatically get the priority from _WorkItem.kwargs

    We also assign a unique id for each item, to avoid making comparisons on _WorkItem.
    As a side effect, items with the same priority are returned FIFO.
    """

    _counter = itertools.count().__next__

    def put(self, item: Optional[_WorkItem], block=True, timeout=None):
        priority = item.kwargs.pop("priority") if item is not None else 0
        super().put((-priority, self._counter(), item), block, timeout)

    def get(self, block=True, timeout=None) -> Optional[_WorkItem]:
        _p, _c, work_item = super().get(block, timeout)
        return work_item


class PriorityThreadPoolExecutor(ThreadPoolExecutor):
    """Overrides ThreadPoolExecutor to use AutoPriorityQueue

    XXX WARNING: Might break in future versions of Python
    """

    def __init__(self, *args):
        super().__init__(*args)

        self._work_queue = AutoPriorityQueue()


class ThreadedYielder(Iterable):
    """Yields results from multiple threads into a single iterator, ordered by priority.

    To add a source iterator, call ``submit()`` with a function that returns an iterator.
    Priority for the iterator can be provided via the keyword argument 'priority'. (higher runs first)
    """

    def __init__(self, max_workers: Optional[int] = None):
        self._pool = PriorityThreadPoolExecutor(max_workers)
        self._futures = deque()
        self._yield = deque()
        self._exception = None

    def _worker(self, fn, *args, **kwargs):
        try:
            res = fn(*args, **kwargs)
            if res is not None:
                self._yield += res
        except Exception as e:
            self._exception = e

    def submit(self, fn: Callable, *args, priority: int = 0, **kwargs):
        self._futures.append(self._pool.submit(self._worker, fn, *args, priority=priority, **kwargs))

    def __iter__(self) -> Iterator:
        while True:
            if self._exception:
                raise self._exception

            while self._yield:
                yield self._yield.popleft()

            if not self._futures:
                # No more tasks
                return

            if self._futures[0].done():
                self._futures.popleft()
            else:
                sleep(0.001)
