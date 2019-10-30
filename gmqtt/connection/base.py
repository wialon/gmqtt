from .options import ConnectionOption


class _BaseConnectionImpl(object):
    async def write(self, data: bytes):
        raise NotImplementedError

    async def read(self, n=-1) -> bytes:
        raise NotImplementedError

    def is_closing(self):
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError


class MQTTConnection(object):

    def __init__(self, transport, protocol, clean_session, keepalive):

        self._clean_session = clean_session
        self._keepalive = keepalive

        self._impl = transport
        self._proto = protocol

        self._handler = None

    @property
    def _protocol(self):
        return self._proto

    async def close(self):
        self._proto.connection_lost(None)
        return await self._impl.close()

    def set_handler(self, handler):
        self._handler = handler

    def put_package(self, pkg):
        self._handler(*pkg)

    async def auth(self, client_id, username, password, will_message=None, **kwargs):
        await self._proto.send_auth_package(client_id, username, password, True,
                                            60, will_message=will_message, **kwargs)

    def publish(self, message):
        return self._proto.send_publish(message)

    def send_disconnect(self, reason_code=0, **properties):
        self._proto.send_disconnect(reason_code=reason_code, **properties)

    def subscribe(self, subscription, **kwargs):
        return self._protocol.send_subscribe_packet(subscription, **kwargs)

    def unsubscribe(self, topic, **kwargs):
        return self._proto.send_unsubscribe_packet(topic, **kwargs)

    def send_simple_command(self, cmd):
        self._proto.send_simple_command_packet(cmd)

    def send_command_with_mid(self, cmd, mid, dup, reason_code=0):
        self._proto.send_command_with_mid(cmd, mid, dup, reason_code=reason_code)

    def _send_ping_request(self):
        self._proto.send_ping_request()

    async def _read(self, n) -> bytes:
        return await self._impl.read(n)

    async def read(self, n=-1) -> bytes:
        return await self._read(n)

    def _write(self, data: bytes):
        return self._impl.write(data)

    def write(self, data: bytes):
        self._write(data)

    def is_closing(self):
        return self._impl.is_closing()

    @classmethod
    async def create_connection(cls, protocol, url, ssl, clean_session, keepalive):
        from .websocket import WebSocketConnectionImpl
        from .tcp import TCPConnectionImpl

        options = ConnectionOption(url, ssl)

        if options.scheme in ('ws', 'wss'):
            connection_impl = WebSocketConnectionImpl
        elif options.scheme == 'tcp':
            connection_impl = TCPConnectionImpl
        elif options.scheme == 'udp':
            connection_impl = TCPConnectionImpl
        else:
            raise ValueError("Invalid transport protocol")

        conn_impl = await connection_impl.create_connection(options)
        conn = cls(conn_impl, protocol, clean_session, keepalive)
        protocol.connection_made(conn)
        return conn
