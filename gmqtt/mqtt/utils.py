import struct


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
