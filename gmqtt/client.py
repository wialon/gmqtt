import asyncio
import heapq
import logging
import uuid

from .mqtt.protocol import MQTTProtocol
from .mqtt.connection import MQTTConnection
from .mqtt.handler import MqttPackageHandler
from .mqtt.constants import MQTTv311, MQTTv50

logger = logging.getLogger(__name__)

FAILED_CONNECTIONS_STOP_RECONNECT = 100

RECONNECTION_SLEEP = 6


class Client(MqttPackageHandler):
    def __init__(self, client_id, clean_session=True, transport='tcp', **kwargs):
        super(Client, self).__init__(optimistic_acknowledgement=kwargs.pop('optimistic_acknowledgement', True))
        self._client_id = client_id or uuid.uuid4().hex

        self._clean_session = clean_session
        self._transport = transport

        self._connection = None

        self._username = None
        self._password = None

        self._host = None
        self._port = None

        self._connect_properties = kwargs
        self._connack_properties = {}

        self._messages_query = []

        self._retry_deliver_timeout = kwargs.get('retry_deliver_timeout', 5)

        asyncio.ensure_future(self._resend_qos_messages())

    def _remove_message_from_query(self, mid):
        message = next(filter(lambda x: x[1] == mid, self._messages_query), None)

        if message:
            self._messages_query.remove(message)
            self._messages_query = heapq.heapify(self._messages_query)
        else:
            logger.warning('[RECEIVED PUBACK FOR UNKNOWN MSG]')

    async def _resend_qos_messages(self):
        loop = asyncio.get_event_loop()

        if not self._messages_query:
            logger.debug('[QoS query IS EMPTY]')
            await asyncio.sleep(self._retry_deliver_timeout)
        else:
            logger.debug('[Some msg need to resend]')
            (tm, mid, package) = heapq.heappop(self._messages_query)
            current_time = loop.time()

            if current_time - tm > self._retry_deliver_timeout:
                logger.debug('[TRY TO RESEND MSG] mid: %s', mid)
                try:
                    self._connection.send_package(package)
                except Exception as exc:
                    logger.error('[ERROR WHILE RESENDING] mid: %s', mid, exc_info=exc)
            else:
                await asyncio.sleep(self._retry_deliver_timeout)

            heapq.heappush(self._messages_query, (current_time, mid, package))

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

        MQTTProtocol.proto_ver = version

        self._connection = await self._create_connection(host, port=self._port, clean_session=self._clean_session, keepalive=keepalive)

        await self._connection.auth(self._client_id, self._username, self._password, **self._connect_properties)
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
        if self.failed_connections > FAILED_CONNECTIONS_STOP_RECONNECT:
            logger.error('[DISCONNECTED] max number of failed connection attempts achieved')
            return
        await asyncio.sleep(RECONNECTION_SLEEP)
        self._connection = await self._create_connection(self._host, self._port, clean_session=True, keepalive=60)
        await self._connection.auth(self._client_id, self._username, self._password, **self._connect_properties)

    async def disconnect(self):
        self._reconnect = False
        if self._connection:
            await self._connection.close()

    def subscribe(self, topic, qos=0, **kwargs):
        self._connection.subsribe(topic, qos, **kwargs)

    def publish(self, topic, payload, qos=0, retain=False, **kwargs):
        loop = asyncio.get_event_loop()

        mid, package = self._connection.publish(topic, payload, qos=qos, retain=retain, **kwargs)

        if qos > 0:
            msg = (loop.time(), mid, package)
            heapq.heappush(self._messages_query, msg)

    def _send_simple_command(self, cmd):
        self._connection.send_simple_command(cmd)

    def _send_command_with_mid(self, cmd, mid, dup, reason_code=0):
        self._connection.send_command_with_mid(cmd, mid, dup, reason_code=reason_code)

    @property
    def protocol_version(self):
        return self._connection._protocol.proto_ver \
            if self._connection is not None else MQTTv50
