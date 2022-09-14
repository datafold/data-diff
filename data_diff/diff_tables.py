"""Provides classes for performing a table diff
"""

from enum import Enum
from contextlib import contextmanager
import threading
from operator import methodcaller
from typing import Tuple, Iterator, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from runtype import dataclass


class Algorithm(Enum):
    AUTO = "auto"
    JOINDIFF = "joindiff"
    HASHDIFF = "hashdiff"


DiffResult = Iterator[Tuple[str, tuple]]  # Iterator[Tuple[Literal["+", "-"], tuple]]


@dataclass
class ThreadBase:
    "Provides utility methods for optional threading"

    threaded: bool = True
    max_threadpool_size: Optional[int] = 1

    def _thread_map(self, func, iterable):
        if not self.threaded:
            return map(func, iterable)

        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            return task_pool.map(func, iterable)

    def _threaded_call(self, func, iterable):
        "Calls a method for each object in iterable."
        return list(self._thread_map(methodcaller(func), iterable))

    def _thread_as_completed(self, func, iterable):
        if not self.threaded:
            yield from map(func, iterable)
            return

        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            futures = [task_pool.submit(func, item) for item in iterable]
            for future in as_completed(futures):
                yield future.result()

    def _threaded_call_as_completed(self, func, iterable):
        "Calls a method for each object in iterable. Returned in order of completion."
        return self._thread_as_completed(methodcaller(func), iterable)

    @contextmanager
    def _run_in_background(self, *funcs):
        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            futures = [task_pool.submit(f) for f in funcs]
            yield futures
            for f in futures:
                f.result()
