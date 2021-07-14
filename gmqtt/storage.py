import asyncio
from typing import Callable, Tuple, Set

import heapq


class BasePersistentStorage(object):
    async def push_message(self, mid, raw_package):
        raise NotImplementedError

    def push_message_nowait(self, mid, raw_package) -> asyncio.Future:
        return asyncio.ensure_future(self.push_message(mid, raw_package))

    async def pop_message(self) -> Tuple[int, bytes]:
        raise NotImplementedError

    async def remove_message_by_mid(self, mid):
        raise NotImplementedError

    @property
    async def is_empty(self) -> bool:
        raise NotImplementedError

    async def wait_empty(self) -> None:
        # Note that when some kinda really persistent storage is used (like Redis or smth),
        # this method must implement an atomic transaction from async network exchange perspective.
        raise NotImplementedError


class HeapPersistentStorage(BasePersistentStorage):
    def __init__(self, timeout):
        self._queue = []
        self._timeout = timeout
        self._empty_waiters: Set[asyncio.Future] = set()

    def _notify_waiters(self, waiters: Set[asyncio.Future], notify: Callable[[asyncio.Future], None]) -> None:
        while waiters:
            notify(waiters.pop())

    def _check_empty(self):
        if not self._queue:
            self._notify_waiters(self._empty_waiters, lambda waiter: waiter.set_result(None))

    async def push_message(self, mid, raw_package):
        tm = asyncio.get_event_loop().time()
        heapq.heappush(self._queue, (tm, mid, raw_package))

    async def pop_message(self):
        current_time = asyncio.get_event_loop().time()

        (tm, mid, raw_package) = heapq.heappop(self._queue)

        if current_time - tm > self._timeout:
            self._check_empty()
            return mid, raw_package
        else:
            heapq.heappush(self._queue, (tm, mid, raw_package))

        return None

    async def remove_message_by_mid(self, mid):
        message = next(filter(lambda x: x[1] == mid, self._queue), None)
        if message:
            self._queue.remove(message)
            self._check_empty()
        heapq.heapify(self._queue)

    @property
    async def is_empty(self):
        return not bool(self._queue)

    async def wait_empty(self) -> None:
        if self._queue:
            waiter = asyncio.get_running_loop().create_future()
            self._empty_waiters.add(waiter)
            await waiter
