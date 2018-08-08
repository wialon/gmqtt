import asyncio
from typing import Tuple

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


class HeapPersistentStorage(BasePersistentStorage):
    def __init__(self, timeout):
        self._queue = []
        self._timeout = timeout

    async def push_message(self, mid, raw_package):
        tm = asyncio.get_event_loop().time()
        heapq.heappush(self._queue, (tm, mid, raw_package))

    async def pop_message(self):
        current_time = asyncio.get_event_loop().time()

        (tm, mid, raw_package) = heapq.heappop(self._queue)

        if current_time - tm > self._timeout:
            return mid, raw_package
        else:
            heapq.heappush(self._queue, (tm, mid, raw_package))

        return None

    async def remove_message_by_mid(self, mid):
        message = next(filter(lambda x: x[1] == mid, self._queue), None)
        if message:
            self._queue.remove(message)
        heapq.heapify(self._queue)

    @property
    async def is_empty(self):
        return not bool(self._queue)
