import asyncio
import json

import logging
import uuid

from .mqtt.protocol import MQTTProtocol
from .mqtt.connection import MQTTConnection
from .mqtt.handler import MqttPackageHandler
from .mqtt.constants import MQTTv311, MQTTv50


from .storage import HeapPersistentStorage

logger = logging.getLogger(__name__)

FAILED_CONNECTIONS_STOP_RECONNECT = 100

RECONNECTION_SLEEP = 6


class Message:
    def __init__(self, topic, payload, qos=0, retain=False, **kwargs):
        self.topic = topic
        self.qos = qos
        self.retain = retain
        self.dup = False
        self.properties = kwargs

        if isinstance(payload, (list, tuple, dict)):
            payload = json.dumps(payload)

        if isinstance(payload, (int, float)):
            self.payload = str(payload).encode('ascii')
        elif isinstance(payload, str):
            self.payload = payload.encode()
        elif payload is None:
            self.payload = b''
        else:
            self.payload = payload

        self.payload_size = len(self.payload)

        if self.payload_size > 268435455:
            raise ValueError('Payload too large.')


class Client(MqttPackageHandler):
    def __init__(self, client_id, clean_session=True, transport='tcp', optimistic_acknowledgement=True,
                 will_message=None, **kwargs):
        super(Client, self).__init__(optimistic_acknowledgement=optimistic_acknowledgement)
        self._client_id = client_id or uuid.uuid4().hex

        self._clean_session = clean_session
        self._transport = transport

        self._connection = None
        self._keepalive = 60

        self._username = None
        self._password = None

        self._host = None
        self._port = None

        self._connect_properties = kwargs
        self._connack_properties = {}

        self._will_message = will_message

        self._retry_deliver_timeout = kwargs.pop('retry_deliver_timeout', 5)
        self._persistent_storage = kwargs.pop('persistent_storage', HeapPersistentStorage(self._retry_deliver_timeout))

        self._topic_alias_maximum = kwargs.get('topic_alias_maximum', 0)

        asyncio.ensure_future(self._resend_qos_messages())

    def _remove_message_from_query(self, mid):

        logger.debug('[REMOVE MESSAGE] %s', mid)
        asyncio.ensure_future(
            self._persistent_storage.remove_message_by_mid(mid)
        )

    async def _resend_qos_messages(self):
        await self._connected.wait()

        if await self._persistent_storage.is_empty:
            logger.debug('[QoS query IS EMPTY]')
            await asyncio.sleep(self._retry_deliver_timeout)
        else:
            logger.debug('[Some msg need to resend]')
            msg = await self._persistent_storage.pop_message()

            if msg:
                (mid, package) = msg

                try:
                    self._connection.send_package(package)
                except Exception as exc:
                    logger.error('[ERROR WHILE RESENDING] mid: %s', mid, exc_info=exc)

                await asyncio.sleep(0.001)
                await self._persistent_storage.push_message(mid, package)
            else:
                await asyncio.sleep(self._retry_deliver_timeout)

        asyncio.ensure_future(self._resend_qos_messages())

    @property
    def properties(self):
        # merge two dictionaries from connect and connack packages
        return {**self._connack_properties, **self._connect_properties}

    def set_auth_credentials(self, username, password=None):
        self._username = username.encode()
        self._password = password
        if isinstance(self._password, str):
            self._password = password.encode()

    async def connect(self, host, port=1883, keepalive=60, version=MQTTv50):
        # Init connection
        self._host = host
        self._port = port
        self._keepalive = keepalive

        MQTTProtocol.proto_ver = version

        self._connection = await self._create_connection(
            host, port=self._port, clean_session=self._clean_session, keepalive=keepalive)

        await self._connection.auth(self._client_id, self._username, self._password, will_message=self._will_message,
                                    **self._connect_properties)
        await self._connected.wait()

        loop = asyncio.get_event_loop()
        while not await self._persistent_storage.is_empty:
            await loop.create_future()

        if self._error:
            raise self._error

    async def _create_connection(self, host, port, clean_session, keepalive):
        self._reconnect = True
        connection = await MQTTConnection.create_connection(host, port, clean_session, keepalive)
        connection.set_handler(self)
        return connection

    async def reconnect(self):
        await self.disconnect()
        if self.failed_connections > FAILED_CONNECTIONS_STOP_RECONNECT:
            logger.error('[DISCONNECTED] max number of failed connection attempts achieved')
            return
        await asyncio.sleep(RECONNECTION_SLEEP)
        try:
            self._connection = await self._create_connection(self._host, self._port, clean_session=False, keepalive=self._keepalive)
        except OSError as exc:
            self.failed_connections += 1
            logger.warning("[CAN'T RECONNECT] %s", self.failed_connections)
            asyncio.ensure_future(self.reconnect())
            return

        await self._connection.auth(self._client_id, self._username, self._password,
                                    will_message=self._will_message, **self._connect_properties)

    async def disconnect(self, reason_code=0, **properties):
        self._reconnect = False
        if self._connection:
            self._connection.send_disconnect(reason_code=reason_code, **properties)
            await self._connection.close()

    def subscribe(self, topic, qos=0, **kwargs):
        self._connection.subscribe(topic, qos, **kwargs)

    def unsubscribe(self, topic, **kwargs):
        self._connection.unsubscribe(topic, **kwargs)

    def publish(self, message_or_topic, payload=None, qos=0, retain=False, **kwargs):
        loop = asyncio.get_event_loop()

        if isinstance(message_or_topic, Message):
            message = message_or_topic
        else:
            message = Message(message_or_topic, payload, qos=qos, retain=retain, **kwargs)

        mid, package = self._connection.publish(message)

        if qos > 0:
            self._persistent_storage.push_message_nowait(mid, package)

    def _send_simple_command(self, cmd):
        self._connection.send_simple_command(cmd)

    def _send_command_with_mid(self, cmd, mid, dup, reason_code=0):
        self._connection.send_command_with_mid(cmd, mid, dup, reason_code=reason_code)

    @property
    def protocol_version(self):
        return self._connection._protocol.proto_ver \
            if self._connection is not None else MQTTv50
