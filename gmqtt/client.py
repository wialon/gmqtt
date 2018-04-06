import asyncio
import uuid

from .mqtt.protocol import MQTTProtocol
from .mqtt.connection import MQTTConnection
from .mqtt.handler import MqttPackageHandler
from .mqtt.constants import MQTTv311, MQTTv50


class Client(MqttPackageHandler):
    def __init__(self, client_id, clean_session=True, transport='tcp'):
        super(Client, self).__init__()
        self._client_id = client_id or uuid.uuid4().hex

        self._clean_session = clean_session
        self._transport = transport

        self._connection = None

        self._username = None
        self._password = None

        self._host = None
        self._port = None

    def set_auth_credentials(self, username, password=None):
        self._username = username.encode()
        self._password = password
        if isinstance(self._password, str):
            self._password = password.encode()

    async def connect(self, host, port=1883, clean_session=True, keepalive=60, version=MQTTv50):
        # Init connection
        self._host = host
        self._port = port

        MQTTProtocol.proto_ver = version

        self._connection = await self._create_connection(host, port=self._port, clean_session=clean_session, keepalive=keepalive)

        await self._connection.auth(self._client_id, self._username, self._password)
        await self._connected.wait()

        if self._error:
            raise self._error

    async def _create_connection(self, host, port, clean_session, keepalive):
        self._reconnect = True
        connection = await MQTTConnection.create_connection(host, port, clean_session, keepalive)
        connection.set_handler(self)
        return connection

    async def reconnect(self):
        await self.disconnect()
        await asyncio.sleep(1)
        self._connection = await self._create_connection(self._host, self._port, clean_session=True, keepalive=60)
        await self._connection.auth(self._client_id, self._username, self._password)

    async def disconnect(self):
        self._reconnect = False
        if self._connection:
            await self._connection.close()

    def subscribe(self, topic, qos=0, **kwargs):
        self._connection.subsribe(topic, qos, **kwargs)

    def publish(self, topic, payload, qos=0, retain=False, **kwargs):
        self._connection.publish(topic, payload, qos=qos, retain=retain, **kwargs)

    def _send_simple_command(self, cmd):
        self._connection.send_simple_command(cmd)

    def _send_command_with_mid(self, cmd, mid, dup):
        self._connection.send_command_with_mid(cmd, mid, dup)

    @property
    def protocol_version(self):
        return self._connection._protocol.proto_ver \
            if self._connection is not None else MQTTv50
