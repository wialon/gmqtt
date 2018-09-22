import asyncio
import logging
import struct
import time
from collections import defaultdict

from .utils import unpack_variable_byte_integer, IdGenerator
from .property import Property
from .protocol import MQTTProtocol
from .constants import MQTTCommands, PubAckReasonCode, PubRecReasonCode
from .constants import MQTTv311, MQTTv50

logger = logging.getLogger(__name__)


def _empty_callback(*args, **kwargs):
    pass


class MQTTError(Exception):
    pass


class MQTTConnectError(MQTTError):
    __messages__ = {
        1: "Connection Refused: unacceptable protocol version",
        2: "Connection Refused: identifier rejected",
        3: "Connection Refused: broker unavailable",
        4: "Connection Refused: bad user name or password",
        5: "Connection Refused: not authorised",
        10: 'Cannot handle CONNACK package'
    }

    def __init__(self, code):
        self._code = code
        self.message = self.__messages__.get(code, 'Unknown error')\


    def __str__(self):
        return "code {} ({})".format(self._code, self.message)


class EventCallback(object):
    def __init__(self, *args, **kwargs):
        super(EventCallback, self).__init__()

        self._connected = asyncio.Event()

        self._on_connected_callback = _empty_callback
        self._on_disconnected_callback = _empty_callback
        self._on_message_callback = _empty_callback
        self._on_subscribe_callback = _empty_callback

        self._reconnect = True

        self.failed_connections = 0

    @property
    def on_subscribe(self):
        return self._on_subscribe_callback

    @on_subscribe.setter
    def on_subscribe(self, cb):
        if not callable(cb):
            raise ValueError
        self._on_subscribe_callback = cb

    @property
    def on_connect(self):
        return self._on_connected_callback

    @on_connect.setter
    def on_connect(self, cb):
        if not callable(cb):
            raise ValueError
        self._on_connected_callback = cb

    @property
    def on_message(self):
        return self._on_message_callback

    @on_message.setter
    def on_message(self, cb):
        if not callable(cb):
            raise ValueError
        self._on_message_callback = cb

    @property
    def on_disconnect(self):
        return self._on_disconnected_callback

    @on_disconnect.setter
    def on_disconnect(self, cb):
        if not callable(cb):
            raise ValueError
        self._on_disconnected_callback = cb


class MqttPackageHandler(EventCallback):
    def __init__(self, *args, **kwargs):
        super(MqttPackageHandler, self).__init__(*args, **kwargs)
        self._messages_in = {}
        self._handler_cache = {}
        self._error = None
        self._connection = None

        self._id_generator = IdGenerator(max=kwargs.get('receive_maximum', 65535))

        if self.protocol_version == MQTTv50:
            self._optimistic_acknowledgement = kwargs.get('optimistic_acknowledgement', True)
        else:
            self._optimistic_acknowledgement = True

    def _send_command_with_mid(self, cmd, mid, dup, reason_code=0):
        raise NotImplementedError

    def _remove_message_from_query(self, mid):
        raise NotImplementedError

    def _send_puback(self, mid, reason_code=0):
        self._send_command_with_mid(MQTTCommands.PUBACK, mid, False, reason_code=reason_code)

    def _send_pubrec(self, mid, reason_code=0):
        self._send_command_with_mid(MQTTCommands.PUBREC, mid, False, reason_code=reason_code)

    def _send_pubrel(self, mid, dup, reason_code=0):
        self._send_command_with_mid(MQTTCommands.PUBREL | 2, mid, dup, reason_code=reason_code)

    def __get_handler__(self, cmd):
        cmd_type = cmd & 0xF0
        if cmd_type not in self._handler_cache:
            handler_name = '_handle_{}_packet'.format(MQTTCommands(cmd_type).name.lower())
            self._handler_cache[cmd_type] = getattr(self, handler_name, self._default_handler)
        return self._handler_cache[cmd_type]

    def _handle_packet(self, cmd, packet):
        logger.debug('[CMD %s] %s', hex(cmd), packet)
        handler = self.__get_handler__(cmd)
        handler(cmd, packet)
        self._last_msg_in = time.monotonic()

    def _handle_exception_in_future(self, future):
        if not future.exception():
            return
        self.on_disconnect(self, packet=None, exc=future.exception())

    def _default_handler(self, cmd, packet):
        logger.warning('[UNKNOWN CMD] %s %s', hex(cmd), packet)

    def _handle_disconnect_packet(self, cmd, packet):
        if self._reconnect:
            future = asyncio.ensure_future(self.reconnect())
            future.add_done_callback(self._handle_exception_in_future)
        self.on_disconnect(self, packet)

    def _parse_properties(self, packet):
        if self.protocol_version < MQTTv50:
            # If protocol is version is less than 5.0, there is no properties in packet
            return {}, packet
        properties_len, left_packet = unpack_variable_byte_integer(packet)
        packet = left_packet[:properties_len]
        left_packet = left_packet[properties_len:]
        properties_dict = defaultdict(list)
        while packet:
            property_identifier, = struct.unpack("!B", packet[:1])
            property_obj = Property.factory(id_=property_identifier)
            if property_obj is None:
                logger.critical('[PROPERTIES] received invalid property id {}, disconnecting'.format(property_identifier))
                return None, None
            result, packet = property_obj.loads(packet[1:])
            for k, v in result.items():
                properties_dict[k].append(v)
        properties_dict = dict(properties_dict)
        return properties_dict, left_packet

    def _handle_connack_packet(self, cmd, packet):
        self._connected.set()

        (flags, result) = struct.unpack("!BB", packet[:2])

        if result != 0:
            logger.error('[CONNACK] %s', hex(result))
            self.failed_connections += 1
            if result == 1 and self.protocol_version == MQTTv50:
                MQTTProtocol.proto_ver = MQTTv311
                future = asyncio.ensure_future(self.reconnect())
                future.add_done_callback(self._handle_exception_in_future)
                return
            else:
                self._error = MQTTConnectError(result)
                asyncio.ensure_future(self.reconnect())
                return
        else:
            self.failed_connections = 0

        if len(packet) > 2:
            properties, _ = self._parse_properties(packet[2:])
            if properties is None:
                self._error = MQTTConnectError(10)
                asyncio.ensure_future(self.disconnect())
            self._connack_properties = properties

        # TODO: Implement checking for the flags and results
        # see 3.2.2.3 Connect Return code of the http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf

        logger.debug('[CONNACK] flags: %s, result: %s', hex(flags), hex(result))
        self.on_connect(self, flags, result, self.properties)

    def _handle_publish_packet(self, cmd, raw_packet):
        header = cmd

        dup = (header & 0x08) >> 3
        qos = (header & 0x06) >> 1
        retain = header & 0x01

        pack_format = "!H" + str(len(raw_packet) - 2) + 's'
        (slen, packet) = struct.unpack(pack_format, raw_packet)

        pack_format = '!' + str(slen) + 's' + str(len(packet) - slen) + 's'
        (topic, packet) = struct.unpack(pack_format, packet)

        if not topic:
            logger.warning('[MQTT ERR PROTO] topic name is empty')
            return

        try:
            print_topic = topic.decode('utf-8')
        except UnicodeDecodeError as exc:
            logger.warning('[INVALID CHARACTER IN TOPIC] %s', topic, exc_info=exc)
            print_topic = topic

        payload = packet

        logger.debug('[RECV %s with QoS: %s] %s', print_topic, qos, payload)

        if qos > 0:
            pack_format = "!H" + str(len(packet) - 2) + 's'
            (mid, packet) = struct.unpack(pack_format, packet)
        else:
            mid = None

        properties, packet = self._parse_properties(packet)
        if packet is None:
            logger.critical('[INVALID MESSAGE] skipping: {}'.format(raw_packet))
            return

        if qos == 0:
            self.on_message(self, print_topic, packet, qos, properties)
            self._id_generator.free_id(mid)
        elif qos == 1:
            self._handle_qos_1_publish_packet(mid, packet, print_topic, properties)
        elif qos == 2:
            self._handle_qos_2_publish_packet(mid, packet, print_topic, properties)

    def _handle_qos_2_publish_packet(self, mid, packet, print_topic, properties):
        if self._optimistic_acknowledgement:
            self._send_pubrec(mid)
            self.on_message(self, print_topic, packet, 2, properties)
        else:
            reason_code = self.on_message(self, print_topic, packet, 2, properties)
            if reason_code not in (c.value for c in PubRecReasonCode):
                raise ValueError('Invalid PUBREC reason code {}'.format(reason_code))
            self._send_pubrec(mid, reason_code=reason_code)

        self._id_generator.free_id(mid)

    def _handle_qos_1_publish_packet(self, mid, packet, print_topic, properties):
        if self._optimistic_acknowledgement:
            self._send_puback(mid)
            self.on_message(self, print_topic, packet, 1, properties)
        else:
            reason_code = self.on_message(self, print_topic, packet, 1, properties)
            if reason_code not in (c.value for c in PubAckReasonCode):
                raise ValueError('Invalid PUBACK reason code {}'.format(reason_code))
            self._send_puback(mid, reason_code=reason_code)

        self._id_generator.free_id(mid)

    def __call__(self, cmd, packet):
        try:
            result = self._handle_packet(cmd, packet)
        except Exception as exc:
            logger.error('[ERROR HANDLE PKG]', exc_info=exc)
            result = None
        return result

    def _handle_suback_packet(self, cmd, raw_packet):
        pack_format = "!H" + str(len(raw_packet) - 2) + 's'
        (mid, packet) = struct.unpack(pack_format, raw_packet)
        pack_format = "!" + "B" * len(packet)
        granted_qos = struct.unpack(pack_format, packet)

        logger.info('[SUBACK] %s %s', mid, granted_qos)
        self.on_subscribe(self, mid, granted_qos)

        self._id_generator.free_id(mid)

    def _handle_pingreq_packet(self, cmd, packet):
        logger.info('[PING REQUEST] %s %s', hex(cmd), packet)
        pass

    def _handle_pingresp_packet(self, cmd, packet):
        logger.info('[PONG REQUEST] %s %s', hex(cmd), packet)

    def _handle_puback_packet(self, cmd, packet):
        (mid, ) = struct.unpack("!H", packet[:2])

        # TODO: For MQTT 5.0 parse reason code and properties

        logger.info('[RECEIVED PUBACK FOR] %s', mid)

        self._id_generator.free_id(mid)
        self._remove_message_from_query(mid)

    def _handle_pubcomp_packet(self, cmd, packet):
        pass

    def _handle_pubrec_packet(self, cmd, packet):
        pass

    def _handle_pubrel_packet(self, cmd, packet):
        mid, = struct.unpack("!H", packet)
        self._id_generator.free_id(mid)

        if mid not in self._messages_in:
            return

        topic, payload, qos = self._messages_in[mid]
