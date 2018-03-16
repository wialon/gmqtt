import json
import struct
import logging

from .constants import MQTTCommands


logger = logging.getLogger(__name__)

LAST_MID = 0


class Packet(object):
    __slots__ = ['cmd', 'data']

    def __init__(self, cmd, data):
        self.cmd = cmd
        self.data = data


class PackageFactory(object):
    @classmethod
    async def parse_package(cls, cmd, package):
        pass

    @classmethod
    def build_package(cls, *args, **kwargs) -> bytes:
        raise NotImplementedError

    @classmethod
    def _pack_remaining_length(cls, packet, remaining_length):
        remaining_bytes = []
        while True:
            byte = remaining_length % 128
            remaining_length = remaining_length // 128
            # If there are more digits to encode, set the top bit of this digit
            if remaining_length > 0:
                byte |= 0x80

            remaining_bytes.append(byte)
            packet.append(byte)
            if remaining_length == 0:
                # FIXME - this doesn't deal with incorrectly large payloads
                return packet

    @classmethod
    def _pack_str16(cls, packet, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        packet.extend(struct.pack("!H", len(data)))
        packet.extend(data)

    @classmethod
    def _mid_generate(cls):
        global LAST_MID
        LAST_MID += 1
        if LAST_MID == 65536:
            LAST_MID = 1
        return LAST_MID


class LoginPackageFactor(PackageFactory):
    @classmethod
    def build_package(cls, client_id, username, password, clean_session, keepalive, protocol):
        remaining_length = 2 + len(protocol.proto_name) + 1 + 1 + 2 + 2 + len(client_id)

        connect_flags = 0
        if clean_session:
            connect_flags |= 0x02

        # TODO: does we need this?
        # if cls._will:
        #     remaining_length += 2 + len(cls._will_topic) + 2 + len(cls._will_payload)
        #     connect_flags |= 0x04 | ((cls._will_qos & 0x03) << 3) | ((cls._will_retain & 0x01) << 5)

        if username is not None:
            remaining_length += 2 + len(username)
            connect_flags |= 0x80
            if password is not None:
                connect_flags |= 0x40
                remaining_length += 2 + len(password)

        command = MQTTCommands.CONNECT
        packet = bytearray()
        packet.append(command)

        cls._pack_remaining_length(packet, remaining_length)
        packet.extend(struct.pack("!H" + str(len(protocol.proto_name)) + "sBBH",
                                  len(protocol.proto_name),
                                  protocol.proto_name,
                                  protocol.proto_ver,
                                  connect_flags,
                                  keepalive))

        cls._pack_str16(packet, client_id)

        if username is not None:
            cls._pack_str16(packet, username)

            if password is not None:
                cls._pack_str16(packet, password)

        return packet


class SubscribePacket(PackageFactory):
    @classmethod
    def build_package(cls, topic, qos) -> bytes:
        remaining_length = 2
        if not isinstance(topic, (list, tuple)):
            topics = [topic]
        else:
            topics = topic

        for t in topics:
            remaining_length += 2 + len(t) + 1

        command = MQTTCommands.SUBSCRIBE | (False << 3) | 0x2
        packet = bytearray()
        packet.append(command)
        cls._pack_remaining_length(packet, remaining_length)
        local_mid = cls._mid_generate()
        packet.extend(struct.pack("!H", local_mid))
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
    def build_package(cls, topic, payload, qos, retain, dup=False) -> bytes:
        command = MQTTCommands.PUBLISH | ((dup & 0x1) << 3) | (qos << 1) | (retain & 0x1)
        packet = bytearray()
        packet.append(command)

        if isinstance(payload, dict):
            payload = json.dumps(payload)

        if isinstance(payload, (int, float)):
            payload = str(payload).encode('ascii')
        elif isinstance(payload, str):
            payload = payload.encode()
        elif payload is None:
            payload = b''

        payload_size = len(payload)

        if payload_size > 268435455:
            raise ValueError('Payload too large.')

        remaining_length = 2 + len(topic) + payload_size

        if payload_size == 0:
            logger.debug("Sending PUBLISH (q%d), '%s' (NULL payload)", qos, topic)
        else:
            logger.debug("Sending PUBLISH (q%d), '%s', ... (%d bytes)", qos, topic, payload_size)

        mid = cls._mid_generate()

        if qos > 0:
            # For message id
            remaining_length += 2

        cls._pack_remaining_length(packet, remaining_length)
        cls._pack_str16(packet, topic)

        if qos > 0:
            # For message id
            packet.extend(struct.pack("!H", mid))

        packet.extend(payload)

        return packet


class CommandWithMidPacket(PackageFactory):

    @classmethod
    def build_package(cls, cmd, mid, dup) -> bytes:
        if dup:
            cmd |= 0x8

        remaining_length = 2
        packet = struct.pack('!BBH', cmd, remaining_length, mid)
        return packet