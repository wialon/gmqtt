import asyncio
import logging
import struct

from . import package
from .constants import MQTTv50, MQTTCommands

logger = logging.getLogger(__name__)


class BaseMQTTProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, buffer_size=2**16, loop=None):
        if not loop:
            loop = asyncio.get_event_loop()

        self._connection = None
        self._transport = None

        self._connected = asyncio.Event(loop=loop)

        reader = asyncio.StreamReader(limit=buffer_size, loop=loop)
        super(BaseMQTTProtocol, self).__init__(reader, loop=loop)

    def set_connection(self, conn):
        self._connection = conn

    def _parse_packet(self):
        raise NotImplementedError

    def connection_made(self, transport: asyncio.Transport):
        super(BaseMQTTProtocol, self).connection_made(transport)

        logger.info('[CONNECTION MADE]')
        self._transport = transport

        self._connected.set()

    def data_received(self, data):
        super(BaseMQTTProtocol, self).data_received(data)

    def write_data(self, data: bytes):
        if not self._transport.is_closing():
            self._transport.write(data)
        else:
            logger.warning('[TRYING WRITE TO CLOSED SOCKET]')

    def connection_lost(self, exc):
        self._connected.clear()
        super(BaseMQTTProtocol, self).connection_lost(exc)
        if exc:
            logger.warning('[EXC: CONN LOST]', exc_info=exc)
        else:
            logger.info('[CONN CLOSE NORMALLY]')

    async def read(self, n=-1):
        bs = await self._stream_reader.read(n=n)

        # so we don't receive anything but connection is not closed -
        # let's close it manually
        if not bs and not self._transport.is_closing():
            self._transport.close()
            # self.connection_lost(ConnectionResetError())
            raise ConnectionResetError("Reset connection manually.")
        return bs


class MQTTProtocol(BaseMQTTProtocol):
    proto_name = b'MQTT'
    proto_ver = MQTTv50

    def __init__(self, *args, **kwargs):
        super(MQTTProtocol, self).__init__(*args, **kwargs)
        self._queue = asyncio.Queue()

        self._disconnect = asyncio.Event()

        self._read_loop_future = None

    def connection_made(self, transport: asyncio.Transport):
        super().connection_made(transport)
        self._read_loop_future = asyncio.ensure_future(self._read_loop())

    async def send_auth_package(self, client_id, username, password, clean_session, keepalive,
                                will_message=None, **kwargs):
        pkg = package.LoginPackageFactor.build_package(client_id, username, password, clean_session,
                                                       keepalive, self, will_message=will_message, **kwargs)
        self.write_data(pkg)

    def send_subscribe_packet(self, topic, qos, **kwargs):
        pkg = package.SubscribePacket.build_package(topic, qos, self, **kwargs)
        self.write_data(pkg)

    def send_unsubscribe_packet(self, topic, **kwargs):
        pkg = package.UnsubscribePacket.build_package(topic, self, **kwargs)
        self.write_data(pkg)

    def send_simple_command_packet(self, cmd):
        pkg = package.SimpleCommandPacket.build_package(cmd)
        self.write_data(pkg)

    def send_ping_request(self):
        self.send_simple_command_packet(MQTTCommands.PINGREQ)

    def send_publish(self, message):
        mid, pkg = package.PublishPacket.build_package(message, self)
        self.write_data(pkg)

        return mid, pkg

    def send_disconnect(self, reason_code=0, **properties):
        pkg = package.DisconnectPacket.build_package(self, reason_code=reason_code, **properties)

        self.write_data(pkg)

        return pkg

    def send_command_with_mid(self, cmd, mid, dup, reason_code=0):
        pkg = package.CommandWithMidPacket.build_package(cmd, mid, dup, reason_code=reason_code,
                                                         proto_ver=self.proto_ver)
        self.write_data(pkg)

    async def _read_packet(self):
        remaining_count = []
        remaining_length = 0
        remaining_mult = 1

        while True:
            byte, = struct.unpack("!B", await self.read(1))
            remaining_count.append(byte)

            if len(remaining_count) > 4:
                logger.warning('[MQTT ERR PROTO] RECV MORE THAN 4 bytes for remaining length.')
                return None

            remaining_length += (byte & 127) * remaining_mult
            remaining_mult *= 128

            if byte & 128 == 0:
                break

        packet = b''
        while remaining_length > 0:
            chunk = await self.read(remaining_length)
            remaining_length -= len(chunk)
            packet += chunk

        return packet

    async def _read_loop(self):
        await self._connected.wait()

        while self._connected.is_set():
            try:
                byte = await self.read(1)
                command, = struct.unpack("!B", byte)
                packet = await self._read_packet()
                self._connection.put_package((command, packet))
            except ConnectionResetError as exc:
                # This connection will be closed, because we received the empty data.
                # So we can safely break the while
                logger.debug("[RECV EMPTY] Connection will be reset automatically.")
                break

    def connection_lost(self, exc):
        super(MQTTProtocol, self).connection_lost(exc)
        self._connection.put_package((MQTTCommands.DISCONNECT, b''))
        if self._read_loop_future is not None:
            self._read_loop_future.cancel()
            self._read_loop_future = None

        self._queue = asyncio.Queue()
