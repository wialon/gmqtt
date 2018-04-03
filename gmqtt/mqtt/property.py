import struct


class Property:
    def __init__(self, id_, bytes_struct, name, allowed_packages):
        self.id = id_
        self.bytes_struct = bytes_struct
        self.name = name
        self.allowed_packages = allowed_packages

    def loads(self, bytes_array):
        # returns dict with property name-value and remaining bytes which do not belong to this property
        if self.bytes_struct == 'u8':
            # First two bytes in UTF-8 encoded properties correspond to unicode string length
            str_len, = struct.unpack('!H', bytes_array[:2])
            value = bytes_array[2:2+str_len].decode('utf-8')
            left_str = bytes_array[2+str_len:]

        else:
            value, left_str = self.unpack_helper(self.bytes_struct, bytes_array)
        return {self.name: value}, left_str

    def unpack_helper(self, fmt, data):
        # unpacks property value according to format, returns value and remaining bytes
        size = struct.calcsize(fmt)
        value = struct.unpack(fmt, data[:size])
        left_str = data[size:]
        if len(value) == 1:
            value = value[0]
        return value, left_str

    def dumps(self, data):
        # packs property value into byte array
        packet = bytearray()
        packet.extend(struct.pack('!B', self.id))
        if self.bytes_struct == 'u8':
            if isinstance(data, str):
                data = data.encode('utf-8')
            packet.extend(struct.pack("!H", len(data)))
            packet.extend(data)
            return packet
        packet.extend(struct.pack(self.bytes_struct, data))
        return packet


    @classmethod
    def factory(cls, id_=None, name=None):
        if (name is None and id_ is None) or (name is not None and id_ is not None):
            raise ValueError('Either id or name should be not None')
        if name is not None:
            return PROPERTIES_BY_NAME[name]
        else:
            return PROPERTIES_BY_ID[id_]


PROPERTIES = [
    Property(1, '!B', 'payload_format_id', ['PUBLISH', ]),
    Property(2, '!L', 'message_expiry_interval', ['PUBLISH', ]),
    Property(8, 'u8', 'response_topic', ['PUBLISH', ]),
    Property(17, '!L', 'session_expiry_interval', ['CONNECT', ]),
    Property(24, '!L', 'will_delay_interval', ['CONNECT', ]),
    Property(18, 'u8', 'client_id', ['CONNACK', ]),
    Property(33, '!H', 'receive_maximum', ['CONNECT', ]),
    Property(34, '!H', 'topic_alias_maximum', ['CONNECT', 'CONNACK']),
    Property(35, '!H', 'topic_alias', ['CONNECT', 'CONNACK']),
    Property(36, '!B', 'max_qos', ['CONNACK', ]),
    Property(39, '!L', 'maximum_packet_size', ['CONNECT', 'CONNACK']),
    Property(41, '!B', 'sub_id_available', ['CONNACK', ]),
]

PROPERTIES_BY_ID = {pr.id: pr for pr in PROPERTIES}
PROPERTIES_BY_NAME = {pr.name: pr for pr in PROPERTIES}


