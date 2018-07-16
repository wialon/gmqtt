import struct
import logging


logger = logging.getLogger(__name__)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class IdGenerator(object, metaclass=Singleton):
    def __init__(self, max=65536):
        self._max = max
        self._used_ids = set()
        self._last_used_id = 0

    def _mid_generate(self):
        done = False

        while not done:
            if len(self._used_ids) >= self._max - 1:
                raise OverflowError("All ids has already used. May be your QoS query is full.")

            self._last_used_id += 1

            if self._last_used_id in self._used_ids:
                continue

            if self._last_used_id == self._max:
                self._last_used_id = 0
                continue

            done = True

        self._used_ids.add(self._last_used_id)
        return self._last_used_id

    def free_id(self, id):
        logger.debug('FREE MID: %s', id)
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
        remaining_bytes.extend(struct.pack('!B', b))
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
            raise ValueError('Malformed Variable Byte Integer')
        multiplier *= 128
        if b & 0x80 == 0:
            break
        i += 1
    return value, bts[i + 1:]


def unpack_utf8(bytes_array):
    str_len, = struct.unpack('!H', bytes_array[:2])
    value = bytes_array[2:2 + str_len].decode('utf-8')
    left_str = bytes_array[2 + str_len:]
    return value, left_str


def pack_utf8(data):
    packet = bytearray()
    if isinstance(data, str):
        data = data.encode('utf-8')
    packet.extend(struct.pack("!H", len(data)))
    packet.extend(data)
    return packet
