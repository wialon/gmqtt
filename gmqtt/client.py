import asyncio
import json

import logging
import uuid
from typing import Union, Sequence

from .mqtt.protocol import MQTTProtocol
from .mqtt.connection import MQTTConnection
from .mqtt.handler import MqttPackageHandler
from .mqtt.constants import MQTTv50, UNLIMITED_RECONNECTS

from .storage import HeapPersistentStorage


class Message:
    def __init__(self, topic, payload, qos=0, retain=False, **kwargs):
        self.topic = topic.encode('utf-8', errors='replace') if isinstance(topic, str) else topic
        self.qos = qos
        self.retain = retain
        self.dup = False
        self.properties = kwargs

        if isinstance(payload, (list, tuple, dict)):
            payload = json.dumps(payload, ensure_ascii=False)

        if isinstance(payload, (int, float)):
            self.payload = str(payload).encode('ascii')
        elif isinstance(payload, str):
            self.payload = payload.encode('utf-8', errors='replace')
        elif payload is None:
            self.payload = b''
        else:
            self.payload = payload

        self.payload_size = len(self.payload)

        if self.payload_size > 268435455:
            raise ValueError('Payload too large.')


class Subscription:
    def __init__(self, topic, qos=0, no_local=False, retain_as_published=False, retain_handling_options=0,
                 subscription_identifier=None):
        self.topic = topic
        self.qos = qos
        self.no_local = no_local
        self.retain_as_published = retain_as_published
        self.retain_handling_options = retain_handling_options

        self.mid = None
        self.acknowledged = False

        # this property can be used only in MQTT5.0
        self.subscription_identifier = subscription_identifier


class SubscriptionsHandler:
    def __init__(self):
        self.subscriptions = []

    def update_subscriptions_with_subscription_or_topic(
            self, subscription_or_topic, qos, no_local, retain_as_published, retain_handling_options, kwargs):

        sentinel = object()
        subscription_identifier = kwargs.get('subscription_identifier', sentinel)

        if isinstance(subscription_or_topic, Subscription):

            if subscription_identifier is not sentinel:
                subscription_or_topic.subscription_identifier = subscription_identifier

            subscriptions = [subscription_or_topic]
        elif isinstance(subscription_or_topic, (tuple, list)):

            if subscription_identifier is not sentinel:
                for sub in subscription_or_topic:
                    sub.subscription_identifier = subscription_identifier

            subscriptions = subscription_or_topic
        elif isinstance(subscription_or_topic, str):

            if subscription_identifier is sentinel:
                subscription_identifier = None

            subscriptions = [Subscription(subscription_or_topic, qos=qos, no_local=no_local,
                                          retain_as_published=retain_as_published,
                                          retain_handling_options=retain_handling_options,
                                          subscription_identifier=subscription_identifier)]
        else:
            raise ValueError('Bad subscription: must be string or Subscription or list of Subscriptions')
        self.subscriptions.extend(subscriptions)
        return subscriptions

    def _remove_subscriptions(self, topic: Union[str, Sequence[str]]):
        if isinstance(topic, str):
            self.subscriptions = [s for s in self.subscriptions if s.topic != topic]
        else:
            self.subscriptions = [s for s in self.subscriptions if s.topic not in topic]

    def subscribe(self, subscription_or_topic: Union[str, Subscription, Sequence[Subscription]],
                  qos=0, no_local=False, retain_as_published=False, retain_handling_options=0, **kwargs):

        # Warn: if you will pass a few subscriptions objects, and each will be have different
        # subscription identifier - the only first will be used as identifier
        # if only you will not pass the identifier in kwargs

        subscriptions = self.update_subscriptions_with_subscription_or_topic(
            subscription_or_topic, qos, no_local, retain_as_published, retain_handling_options, kwargs)
        return self._connection.subscribe(subscriptions, **kwargs)

    def resubscribe(self, subscription: Subscription, **kwargs):
        # send subscribe packet for subscription,that's already in client's subscription list
        if 'subscription_identifier' in kwargs:
            subscription.subscription_identifier = kwargs['subscription_identifier']
        elif subscription.subscription_identifier is not None:
            kwargs['subscription_identifier'] = subscription.subscription_identifier
        return self._connection.subscribe([subscription], **kwargs)

    def unsubscribe(self, topic: Union[str, Sequence[str]], **kwargs):
        self._remove_subscriptions(topic)
        return self._connection.unsubscribe(topic, **kwargs)


class Client(MqttPackageHandler, SubscriptionsHandler):
    def __init__(self, client_id, clean_session=True, optimistic_acknowledgement=True,
                 will_message=None, logger=None, **kwargs):
        super(Client, self).__init__(optimistic_acknowledgement=optimistic_acknowledgement, logger=logger)
        self._client_id = client_id or uuid.uuid4().hex

        # in MQTT 5.0 this is clean start flag
        self._clean_session = clean_session

        self._connection = None
        self._keepalive = 60

        self._username = None
        self._password = None

        self._host = None
        self._port = None
        self._ssl = None

        self._connect_properties = kwargs
        self._connack_properties = {}

        self._will_message = will_message

        # TODO: this constant may be moved to config
        self._retry_deliver_timeout = kwargs.pop('retry_deliver_timeout', 5)
        self._persistent_storage = kwargs.pop('persistent_storage', HeapPersistentStorage(self._retry_deliver_timeout))

        self._topic_alias_maximum = kwargs.get('topic_alias_maximum', 0)

        self._resend_task = asyncio.ensure_future(self._resend_qos_messages())

        self._logger = logger or logging.getLogger(__name__)

    def get_subscription_by_identifier(self, subscription_identifier):
        return next((sub for sub in self.subscriptions if sub.subscription_identifier == subscription_identifier), None)

    def get_subscriptions_by_mid(self, mid):
        return [sub for sub in self.subscriptions if sub.mid == mid]

    def _remove_message_from_query(self, mid):
        self._logger.debug('[REMOVE MESSAGE] %s', mid)
        asyncio.ensure_future(
            self._persistent_storage.remove_message_by_mid(mid)
        )

    @property
    def is_connected(self):
        # tells if connection is alive and CONNACK was received
        return self._connected.is_set() and not self._connection.is_closing()

    async def _resend_qos_messages(self):
        await self._connected.wait()

        if await self._persistent_storage.is_empty:
            self._logger.debug('[QoS query IS EMPTY]')
            await asyncio.sleep(self._retry_deliver_timeout)
        elif self._connection.is_closing():
            self._logger.debug('[Some msg need to resend] Transport is closing, sleeping')
            await asyncio.sleep(self._retry_deliver_timeout)
        else:
            self._logger.debug('[Some msg need to resend] processing message')
            msg = await self._persistent_storage.pop_message()

            if msg:
                (mid, package) = msg

                try:
                    self._connection.send_package(package)
                except Exception as exc:
                    self._logger.error('[ERROR WHILE RESENDING] mid: %s', mid, exc_info=exc)

                await self._persistent_storage.push_message(mid, package)
                await asyncio.sleep(0.001)
            else:
                await asyncio.sleep(self._retry_deliver_timeout)

        self._resend_task = asyncio.ensure_future(self._resend_qos_messages())

    @property
    def properties(self):
        # merge two dictionaries from connect and connack packages
        return {**self._connack_properties, **self._connect_properties}

    def set_auth_credentials(self, username, password=None):
        self._username = username.encode()
        self._password = password
        if isinstance(self._password, str):
            self._password = password.encode()

    async def connect(self, host, port=1883, ssl=False, keepalive=60, version=MQTTv50, raise_exc=True):
        # Init connection
        self._host = host
        self._port = port
        self._ssl = ssl
        self._keepalive = keepalive
        self._is_active = True

        MQTTProtocol.proto_ver = version

        self._connection = await self._create_connection(
            host, port=self._port, ssl=self._ssl, clean_session=self._clean_session, keepalive=keepalive)

        await self._connection.auth(self._client_id, self._username, self._password, will_message=self._will_message,
                                    **self._connect_properties)
        await self._connected.wait()

        await self._persistent_storage.wait_empty()

        if raise_exc and self._error:
            raise self._error

    async def _create_connection(self, host, port, ssl, clean_session, keepalive):
        # important for reconnects, make sure u know what u are doing if wanna change :(
        self._exit_reconnecting_state()
        self._clear_topics_aliases()
        connection = await MQTTConnection.create_connection(host, port, ssl, clean_session, keepalive, logger=self._logger)
        connection.set_handler(self)
        return connection

    def _allow_reconnect(self):
        if self._reconnecting_now or not self._is_active:
            return False
        if self._config['reconnect_retries'] == UNLIMITED_RECONNECTS:
            return True
        if self.failed_connections <= self._config['reconnect_retries']:
            return True
        self._logger.error('[DISCONNECTED] max number of failed connection attempts achieved')
        return False

    async def reconnect(self, delay=False):
        if not self._allow_reconnect():
            return
        # stopping auto-reconnects during reconnect procedure is important, better do not touch :(
        self._temporatily_stop_reconnect()
        try:
            await self._disconnect()
        except:
            self._logger.info('[RECONNECT] ignored error while disconnecting, trying to reconnect anyway')
        if delay:
            await asyncio.sleep(self._config['reconnect_delay'])
        try:
            self._connection = await self._create_connection(self._host, self._port, ssl=self._ssl,
                                                             clean_session=False, keepalive=self._keepalive)
        except OSError as exc:
            self.failed_connections += 1
            self._logger.warning("[CAN'T RECONNECT] %s", self.failed_connections)
            asyncio.ensure_future(self.reconnect(delay=True))
            return
        await self._connection.auth(self._client_id, self._username, self._password,
                                    will_message=self._will_message, **self._connect_properties)

    async def disconnect(self, reason_code=0, **properties):
        self._is_active = False
        self._resend_task.cancel()
        await self._disconnect(reason_code=reason_code, **properties)

    async def _disconnect(self, reason_code=0, **properties):
        self._clear_topics_aliases()

        self._connected.clear()
        if self._connection:
            self._connection.send_disconnect(reason_code=reason_code, **properties)
            await self._connection.close()

    def publish(self, message_or_topic, payload=None, qos=0, retain=False, **kwargs):
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
