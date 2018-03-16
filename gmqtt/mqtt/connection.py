import asyncio

import time

from .protocol import MQTTProtocol


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

        asyncio.get_event_loop().call_later(self._keepalive, self._keep_connection)

    @classmethod
    async def create_connection(cls, host, port, clean_session, keepalive, loop=None):
        loop = loop or asyncio.get_event_loop()
        transport, protocol = await loop.create_connection(MQTTProtocol, host, port)
        return MQTTConnection(transport, protocol, clean_session, keepalive)

    def _keep_connection(self):
        if time.monotonic() - self._last_data_in > self._keepalive:
            self._send_ping_request()
        asyncio.get_event_loop().call_later(self._keepalive, self._keep_connection)

    def put_package(self, pkg):
        self._handler(*pkg)

    def send_package(self, package):
        # This is not blocking operation, because transport place the data
        # to the buffer, and this buffer flushing async
        self._transport.write(package.encode())

    async def auth(self, client_id, username, password):
        await self._protocol.send_auth_package(client_id, username, password, self._clean_session, self._keepalive)

    def publish(self, topic, payload, qos, retain):
        self._protocol.send_publish(topic, payload, qos, retain)

    def subsribe(self, topic, qos):
        self._protocol.send_subscribe_packet(topic, qos)

    def send_simple_command(self, cmd):
        self._protocol.send_simple_command_packet(cmd)

    def send_command_with_mid(self, cmd, mid, dup):
        self._protocol.send_command_with_mid(cmd, mid, dup)

    def _send_ping_request(self):
        self._protocol.send_ping_request()

    def set_handler(self, handler):
        self._handler = handler

    async def close(self):
        self._transport.close()
