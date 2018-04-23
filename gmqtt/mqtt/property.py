import struct

from .utils import unpack_variable_byte_integer, pack_variable_byte_integer, pack_utf8, unpack_utf8


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
            value, left_str = unpack_utf8(bytes_array)
        elif self.bytes_struct == 'u8x2':
            value1, left_str = unpack_utf8(bytes_array)
            value2, left_str = unpack_utf8(left_str)
            value = (value1, value2)
        elif self.bytes_struct == 'b':
            str_len, = struct.unpack('!H', bytes_array[:2])
            value = bytes_array[2:2 + str_len]
            left_str = bytes_array[2 + str_len:]
        elif self.bytes_struct == 'vbi':
            value, left_str = unpack_variable_byte_integer(bytes_array)
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
            packet.extend(pack_utf8(data))
            return packet
        elif self.bytes_struct == 'u8x2':
            data1, data2 = data
            packet.extend(pack_utf8(data1))
            packet.extend(pack_utf8(data2))
            return packet
        elif self.bytes_struct == 'b':
            packet.extend(struct.pack('!H', len(data)))
            packet.extend(data)
            return packet
        elif self.bytes_struct == 'vbi':
            packet.extend(pack_variable_byte_integer(data))
            return packet
        packet.extend(struct.pack(self.bytes_struct, data))
        return packet

    @classmethod
    def factory(cls, id_=None, name=None):
        if (name is None and id_ is None) or (name is not None and id_ is not None):
            raise ValueError('Either id or name should be not None')
        if name is not None:
            return PROPERTIES_BY_NAME.get(name)
        else:
            return PROPERTIES_BY_ID.get(id_)


PROPERTIES = [
    Property(1, '!B', 'payload_format_id', ['PUBLISH', ]),
    Property(2, '!L', 'message_expiry_interval', ['PUBLISH', ]),
    Property(3, 'u8', 'content_type', ['PUBLISH']),
    Property(8, 'u8', 'response_topic', ['PUBLISH', ]),
    Property(9, 'b', 'correlation_data', ['PUBLISH']),
    Property(11, 'vbi', 'subscription_identifier', ['PUBLISH', 'SUBSCRIBE']),
    Property(17, '!L', 'session_expiry_interval', ['CONNECT', ]),
    Property(18, 'u8', 'client_id', ['CONNACK', ]),
    Property(19, '!H', 'server_keep_alive', ['CONNACK']),
    Property(21, 'u8', 'auth_method', ['CONNECT', 'CONNACK', 'AUTH']),
    Property(23, '!B', 'request_problem_info', ['CONNECT']),
    Property(24, '!L', 'will_delay_interval', ['CONNECT', ]),
    Property(25, '!B', 'request_response_info', ['CONNECT']),
    Property(26, 'u8', 'response_info', ['CONNACK']),
    Property(28, 'u8', 'server_reference', ['CONNACK', 'DISCONNECT']),
    Property(31, 'u8', 'reason_string', ['CONNACK', 'PUBACK', 'PUBREC', 'PUBREL', 'PUBCOMP', 'SUBACK', 'UNSUBACK',
                                         'DISCONNECT', 'AUTH']),
    Property(33, '!H', 'receive_maximum', ['CONNECT', 'CONNACK']),
    Property(34, '!H', 'topic_alias_maximum', ['CONNECT', 'CONNACK']),
    Property(35, '!H', 'topic_alias', ['CONNECT', 'CONNACK']),
    Property(36, '!B', 'max_qos', ['CONNACK', ]),
    Property(37, '!B', 'retain_available', ['CONNACK', ]),
    Property(38, 'u8x2', 'user_property', ['CONNECT', 'CONNACK', 'PUBLISH', 'PUBACK', 'PUBREC', 'PUBREL',
                                         'PUBCOMP', 'SUBACK', 'UNSUBACK', 'DISCONNECT', 'AUTH']),
    Property(39, '!L', 'maximum_packet_size', ['CONNECT', 'CONNACK']),
    Property(40, '!B', 'wildcard_subscription_available', ['CONNACK']),
    Property(41, '!B', 'sub_id_available', ['CONNACK', ]),
    Property(42, '!B', 'shared_subscription_available', ['CONNACK']),
]

PROPERTIES_BY_ID = {pr.id: pr for pr in PROPERTIES}
PROPERTIES_BY_NAME = {pr.name: pr for pr in PROPERTIES}


