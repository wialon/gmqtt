import struct
import logging
from typing import Tuple

from .constants import MQTTCommands, MQTTv50
from .property import Property
from .utils import pack_variable_byte_integer, IdGenerator

logger = logging.getLogger(__name__)

LAST_MID = 0
USED_IDS = set()


class Packet(object):
    __slots__ = ['cmd', 'data']

    def __init__(self, cmd, data):
        self.cmd = cmd
        self.data = data


class PackageFactory(object):
    id_generator = IdGenerator()

    @classmethod
    async def parse_package(cls, cmd, package):
        pass

    @classmethod
    def build_package(cls, *args, **kwargs) -> bytes:
        raise NotImplementedError

    @classmethod
    def _pack_str16(cls, packet, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        packet.extend(struct.pack("!H", len(data)))
        packet.extend(data)

    @classmethod
    def _build_properties_data(cls, properties_dict, protocol_version):
        if protocol_version < MQTTv50:
            return bytearray()
        data = bytearray()
        for property_name, property_value in properties_dict.items():
            property = Property.factory(name=property_name)
            if property is None:
                logger.warning('[GMQTT] property {} is not supported, it was ignored'.format(property_name))
                continue
            property_bytes = property.dumps(property_value)
            data.extend(property_bytes)
        result = pack_variable_byte_integer(len(data))
        result.extend(data)
        return result


class LoginPackageFactor(PackageFactory):
    @classmethod
    def build_package(cls, client_id, username, password, clean_session, keepalive, protocol, will_message=None, **kwargs):
        remaining_length = 2 + len(protocol.proto_name) + 1 + 1 + 2 + 2 + len(client_id)

        connect_flags = 0
        if clean_session:
            connect_flags |= 0x02

        if will_message:
            will_prop_bytes = cls._build_properties_data(will_message.properties, protocol.proto_ver)
            remaining_length += 2 + len(will_message.topic) + 2 + len(will_message.payload) + len(will_prop_bytes)
            connect_flags |= 0x04 | ((will_message.qos & 0x03) << 3) | ((will_message.retain & 0x01) << 5)

        if username is not None:
            remaining_length += 2 + len(username)
            connect_flags |= 0x80
            if password is not None:
                connect_flags |= 0x40
                remaining_length += 2 + len(password)

        command = MQTTCommands.CONNECT
        packet = bytearray()
        packet.append(command)

        prop_bytes = cls._build_properties_data(kwargs, protocol.proto_ver)
        remaining_length += len(prop_bytes)

        packet.extend(pack_variable_byte_integer(remaining_length))
        packet.extend(struct.pack("!H" + str(len(protocol.proto_name)) + "sBBH",
                                  len(protocol.proto_name),
                                  protocol.proto_name,
                                  protocol.proto_ver,
                                  connect_flags,
                                  keepalive))

        packet.extend(prop_bytes)

        cls._pack_str16(packet, client_id)

        if will_message:
            packet += will_prop_bytes
            cls._pack_str16(packet, will_message.topic)
            cls._pack_str16(packet, will_message.payload)

        if username is not None:
            cls._pack_str16(packet, username)

            if password is not None:
                cls._pack_str16(packet, password)

        return packet


class UnsubscribePacket(PackageFactory):
    @classmethod
    def build_package(cls, topic, protocol, **kwargs) -> bytes:
        remaining_length = 2
        if not isinstance(topic, (list, tuple)):
            topics = [topic]
        else:
            topics = topic

        for t in topics:
            remaining_length += 2 + len(t)

        properties = cls._build_properties_data(kwargs, protocol.proto_ver)
        remaining_length += len(properties)

        command = MQTTCommands.UNSUBSCRIBE | 0x2
        packet = bytearray()
        packet.append(command)
        packet.extend(pack_variable_byte_integer(remaining_length))
        local_mid = cls.id_generator.next_id()
        packet.extend(struct.pack("!H", local_mid))
        packet.extend(properties)
        for t in topics:
            cls._pack_str16(packet, t)

        logger.info('[SEND UNSUB] %s', topics)

        return packet


class SubscribePacket(PackageFactory):
    @classmethod
    def build_package(cls, topic, qos, protocol, **kwargs) -> bytes:
        remaining_length = 2
        if not isinstance(topic, (list, tuple)):
            topics = [topic]
        else:
            topics = topic

        for t in topics:
            remaining_length += 2 + len(t) + 1

        properties = cls._build_properties_data(kwargs, protocol.proto_ver)
        remaining_length += len(properties)

        command = MQTTCommands.SUBSCRIBE | (False << 3) | 0x2
        packet = bytearray()
        packet.append(command)
        packet.extend(pack_variable_byte_integer(remaining_length))
        local_mid = cls.id_generator.next_id()
        packet.extend(struct.pack("!H", local_mid))
        packet.extend(properties)
        for t in topics:
            cls._pack_str16(packet, t)
            packet.append(qos)

        logger.info('[SEND SUB] %s', topics)

        return packet


class SimpleCommandPacket(PackageFactory):
    @classmethod
    def build_package(cls, command) -> bytes:
        return struct.pack('!BB', command, 0)


class PublishPacket(PackageFactory):
    @classmethod
    def build_package(cls, message, protocol) -> Tuple[int, bytes]:
        command = MQTTCommands.PUBLISH | ((message.dup & 0x1) << 3) | (message.qos << 1) | (message.retain & 0x1)

        packet = bytearray()
        packet.append(command)

        remaining_length = 2 + len(message.topic) + message.payload_size
        prop_bytes = cls._build_properties_data(message.properties, protocol_version=protocol.proto_ver)
        remaining_length += len(prop_bytes)

        if message.payload_size == 0:
            logger.debug("Sending PUBLISH (q%d), '%s' (NULL payload)", message.qos, message.topic)
        else:
            logger.debug("Sending PUBLISH (q%d), '%s', ... (%d bytes)", message.qos, message.topic, message.payload_size)

        if message.qos > 0:
            # For message id
            remaining_length += 2

        packet.extend(pack_variable_byte_integer(remaining_length))
        cls._pack_str16(packet, message.topic)

        if message.qos > 0:
            # For message id
            mid = cls.id_generator.next_id()
            packet.extend(struct.pack("!H", mid))
        else:
            mid = None
        packet.extend(prop_bytes)

        packet.extend(message.payload)

        return mid, packet


class DisconnectPacket(PackageFactory):
    @classmethod
    def build_package(cls, protocol, reason_code=0, **properties):
        prop_bytes = cls._build_properties_data(properties, protocol_version=protocol.proto_ver)
        remaining_length = 1 + len(prop_bytes)
        return struct.pack('!BBB', MQTTCommands.DISCONNECT.value, remaining_length, reason_code) + prop_bytes


class CommandWithMidPacket(PackageFactory):

    @classmethod
    def build_package(cls, cmd, mid, dup, reason_code=0, proto_ver=MQTTv50) -> bytes:
        if dup:
            cmd |= 0x8
        if proto_ver == MQTTv50:
            remaining_length = 4
            packet = struct.pack('!BBHBB', cmd, remaining_length, mid, reason_code, 0)
        else:
            remaining_length = 2
            packet = struct.pack('!BBH', cmd, remaining_length, mid)
        return packet
