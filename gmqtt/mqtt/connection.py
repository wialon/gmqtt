import asyncio
import logging
import time
from functools import partial

from .handler import MqttPackageHandler
from .protocol import MQTTProtocol
from .package import Package
from .utils import ConnectionState

class MQTTConnection:
    def __init__(self, transport: asyncio.Transport, protocol: MQTTProtocol, clean_session: bool, keepalive: int,
                 package_handler: MqttPackageHandler, logger=None):
        self._transport = transport
        self._protocol = protocol
        self._protocol.set_connection(self)
        self._buff = asyncio.Queue()

        self._clean_session = clean_session
        self._keepalive = keepalive

        self._last_data_in = time.monotonic()
        self._last_data_out = time.monotonic()

        self._keep_connection_callback = asyncio.get_event_loop().call_later(self._keepalive / 2, self._keep_connection)

        self._logger = logger or logging.getLogger(__name__)
        self._handler = package_handler
        # The handler outlives individual connections; re-point it at this one.
        self._handler.set_connection(self)

    @classmethod
    async def create_connection(cls, host, port, ssl, clean_session, keepalive, connection_state: ConnectionState,
                                package_handler: MqttPackageHandler, loop=None, logger=None):
        loop = loop or asyncio.get_event_loop()
        protocol_factory = partial(MQTTProtocol, connection_state=connection_state, id_generator=package_handler.id_generator)
        transport, protocol = await loop.create_connection(protocol_factory, host, port, ssl=ssl)
        return MQTTConnection(transport, protocol, clean_session, keepalive, package_handler=package_handler, logger=logger)

    def _keep_connection(self):
        if self.is_closing() or not self._keepalive:
            return

        time_ = time.monotonic()
        if time_ - self._last_data_in >= 2 * self._keepalive:
            self._logger.warning("[LOST HEARTBEAT FOR %s SECONDS, GOING TO CLOSE CONNECTION]", 2 * self._keepalive)
            asyncio.ensure_future(self.close())
            return

        if time_ - self._last_data_out >= 0.8 * self._keepalive or \
                time_ - self._last_data_in >= 0.8 * self._keepalive:
            self._send_ping_request()
        self._keep_connection_callback = asyncio.get_event_loop().call_later(self._keepalive / 2, self._keep_connection)

    def put_package(self, pkg: Package):
        # A superseded connection can still deliver buffered packets, notably
        # the synthetic DISCONNECT from connection_lost. Routing those to the
        # shared handler would abort the current connection's queue drain and
        # schedule a reconnect against a healthy link.
        if self._handler.current_connection is not self:
            self._logger.debug("[RECV] dropping package from superseded connection")
            return
        self._last_data_in = time.monotonic()
        self._handler(pkg)

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

    async def close(self):
        if self._keep_connection_callback:
            self._keep_connection_callback.cancel()
        self._transport.close()
        try:
            await self._protocol.closed
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as exc:
            # The broker closed the socket forcefully (common on TLS after DISCONNECT).
            # App called the close(), so the connection is gone either way.
            # The WARNING is already emitted by connection_lost(); no need to re-raise.
            self._logger.debug("[CLOSE] suppressed transport error after intentional close: %s", exc)

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
