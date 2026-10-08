import asyncio
import inspect
import logging
import struct
from copy import deepcopy
from dataclasses import dataclass, field
from functools import partial
from typing import Callable

from .constants import DEFAULT_CONFIG, MQTTv50

logger = logging.getLogger(__name__)


@dataclass
class ConnectionState:
    protocol_version: int = MQTTv50
    config: dict = field(default_factory=lambda: deepcopy(DEFAULT_CONFIG))
    failed_connections: int = 0
    reconnecting_now: bool = False


class IdGenerator:
    """Allocator for outbound MQTT packet identifiers (PUBLISH/SUBSCRIBE/UNSUBSCRIBE).

    One instance per Client. The pool is NOT a process-wide singleton — two
    Clients in the same process must have independent pools, and inbound mids
    from the broker live in a different namespace and must never be passed to
    free_id.
    """

    def __init__(self, max=65536):
        self._max = max
        self._used_ids = set()
        self._last_used_id = 0

    def _mid_generate(self):
        done = False

        while not done:
            if len(self._used_ids) >= self._max - 1:
                raise OverflowError(
                    "All ids has already used. May be your QoS queue is full."
                )

            self._last_used_id += 1

            if self._last_used_id in self._used_ids:
                continue

            # Wrap on >=, not ==: CONNACK receive_maximum can lower _max
            # below the counter, which would otherwise never hit the bound
            # and climb past 65535 until struct.pack("!H", mid) raises.
            if self._last_used_id >= self._max:
                self._last_used_id = 0
                continue

            done = True

        self._used_ids.add(self._last_used_id)
        return self._last_used_id

    def free_id(self, id):
        logger.debug("FREE MID: %s", id)
        if id not in self._used_ids:
            return

        self._used_ids.remove(id)

    def next_id(self):
        id = self._mid_generate()

        logger.debug("NEW ID: %s", id)
        return id


def pack_variable_byte_integer(value):
    remaining_bytes = bytearray()
    while True:
        value, b = divmod(value, 128)
        if value > 0:
            b |= 0x80
        remaining_bytes.extend(struct.pack("!B", b))
        if value <= 0:
            break
    return remaining_bytes


def unpack_variable_byte_integer(bts):
    multiplier = 1
    value = 0
    i = 0
    while i < 4:
        b = bts[i]
        value += (b & 0x7F) * multiplier
        if multiplier > 2097152:  # 128 * 128 * 128
            raise ValueError("Malformed Variable Byte Integer")
        multiplier *= 128
        if b & 0x80 == 0:
            break
        i += 1
    return value, bts[i + 1 :]


def unpack_utf8(bytes_array):
    (str_len,) = struct.unpack("!H", bytes_array[:2])
    value = bytes_array[2 : 2 + str_len].decode("utf-8")
    left_str = bytes_array[2 + str_len :]
    return value, left_str


def pack_utf8(data):
    packet = bytearray()
    if isinstance(data, str):
        data = data.encode("utf-8")
    packet.extend(struct.pack("!H", len(data)))
    packet.extend(data)
    return packet


def is_coroutine_function_or_partial(obj: Callable):
    while isinstance(obj, partial):
        obj = obj.func

    # inspect.iscoroutinefunction, not asyncio.iscoroutinefunction: the latter
    # is deprecated since 3.14. Since 3.12 inspect's version also recognises
    # callables tagged with markcoroutinefunction.
    return inspect.iscoroutinefunction(obj)


def run_coroutine_or_function(func, *args, callback=None, **kwargs):
    if is_coroutine_function_or_partial(func):
        f = asyncio.ensure_future(func(*args, **kwargs))
        if callback is not None:
            f.add_done_callback(callback)
    else:
        func(*args, **kwargs)
