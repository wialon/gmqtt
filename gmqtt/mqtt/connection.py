import asyncio
import logging
import time

from .protocol import MQTTProtocol

logger = logging.getLogger(__name__)

class MQTTConnection(object):
    def __init__(self, transport: asyncio.Transport, protocol: MQTTProtocol, clean_session: bool, keepalive: int):
        self._transport = transport
        self._protocol = protocol
        self._protocol.set_connection(self)
        self._buff = asyncio.Queue()

        self._clean_session = clean_session
        self._keepalive = keepalive

        self._last_data_in = time.monotonic()
        self._last_data_out = time.monotonic()

        self._keep_connection_callback = asyncio.get_event_loop().call_later(self._keepalive / 2, self._keep_connection)

    @classmethod
    async def create_connection(cls, host, port, ssl, clean_session, keepalive, loop=None):
        loop = loop or asyncio.get_event_loop()
        transport, protocol = await loop.create_connection(MQTTProtocol, host, port, ssl=ssl)
        return MQTTConnection(transport, protocol, clean_session, keepalive)

    def _keep_connection(self):
        if self.is_closing() or not self._keepalive:
            return

        time_ = time.monotonic()
        if time_ - self._last_data_in >= 2 * self._keepalive:
            logger.warning("[LOST HEARTBEAT FOR %s SECONDS, GOING TO CLOSE CONNECTION]", 2 * self._keepalive)
            asyncio.ensure_future(self.close())
            return

        if time_ - self._last_data_out >= 0.8 * self._keepalive or \
                time_ - self._last_data_in >= 0.8 * self._keepalive:
            self._send_ping_request()
        self._keep_connection_callback = asyncio.get_event_loop().call_later(self._keepalive / 2, self._keep_connection)

    def put_package(self, pkg):
        self._last_data_in = time.monotonic()
        self._handler(*pkg)

    def send_package(self, package):
        # This is not blocking operation, because transport place the data
        # to the buffer, and this buffer flushing async
        self._last_data_out = time.monotonic()
        if isinstance(package, (bytes, bytearray)):
            package = package
        else:
            package = package.encode()

        self._transport.write(package)

    async def auth(self, client_id, username, password, will_message=None, **kwargs):
        await self._protocol.send_auth_package(client_id, username, password, self._clean_session,
                                               self._keepalive, will_message=will_message, **kwargs)

    def publish(self, message):
        return self._protocol.send_publish(message)

    def send_disconnect(self, reason_code=0, **properties):
        self._protocol.send_disconnect(reason_code=reason_code, **properties)

    def subscribe(self, subscription, **kwargs):
        return self._protocol.send_subscribe_packet(subscription, **kwargs)

    def unsubscribe(self, topic, **kwargs):
        return self._protocol.send_unsubscribe_packet(topic, **kwargs)

    def send_simple_command(self, cmd):
        self._protocol.send_simple_command_packet(cmd)

    def send_command_with_mid(self, cmd, mid, dup, reason_code=0):
        self._protocol.send_command_with_mid(cmd, mid, dup, reason_code=reason_code)

    def _send_ping_request(self):
        self._protocol.send_ping_request()

    def set_handler(self, handler):
        self._handler = handler

    async def close(self):
        if self._keep_connection_callback:
            self._keep_connection_callback.cancel()
        self._transport.close()
        await self._protocol.closed

    def is_closing(self):
        return self._transport.is_closing()

    @property
    def keepalive(self):
        return self._keepalive

    @keepalive.setter
    def keepalive(self, value):
        if self._keepalive == value:
            return
        self._keepalive = value
        if self._keep_connection_callback:
            self._keep_connection_callback.cancel()
        self._keep_connection_callback = asyncio.get_event_loop().call_later(self._keepalive / 2, self._keep_connection)
