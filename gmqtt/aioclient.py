import asyncio
import collections
from typing import Optional, Tuple, Callable

from .client import Client, Message
from .message_queue import SubscriptionManager


class MqttClientWrapper:
    """
    Wraps the callback-based client in a async/await API
    """

    def __init__(
        self,
        inner_client: Client,
        receive_maximum: Optional[int] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop

        self.client = inner_client

        receive_maximum = receive_maximum or 65665  # FIXME: Sane default?

        self.subscription_manager = SubscriptionManager(receive_maximum)
        # self.message_queue = asyncio.Queue(maxsize=receive_maximum or 0)
        # JKelf._subscriptions = collections.defaultdict(set)
        self._init_client()

    def _init_client(self):
        """Set up client so messages are forwarded to registered subscriptions:"""

        def _on_message(client, topic, payload, qos, properties):
            message = Message(client=client, topic=topic, payload=payload, **properties)
            self.subscription_manager.on_message(message)

        self.client.on_message = _on_message

    async def publish(self, topic: str, message: Message, qos=0):
        """Publish a message to the MQTT topic"""
        self.client.publish(topic, message, qos)

    class Subscribe(collections.abc.Awaitable):
        def __init__(self, client_wrapper, topic, qos=0):
            self.topic = topic
            self.qos = qos
            self.client_wrapper = client_wrapper

        def __await__(self):
            return self.__await_impl__().__await__()

        async def __await_impl__(self):
            subscription = await self.client_wrapper.subscription_manager.add_subscription(
                self.topic
            )
            self.client_wrapper.client.subscribe(self.topic, qos=self.qos)

            return subscription

        async def _unsubscribe(self):
            self.client_wrapper.client.unsubscribe(self.topic)
            # TODO: Await unsubscribe callback

        async def __aenter__(self):
            return await self

        async def __aexit__(self, exc_type, exc_value, traceback):
            # TODO: Wait for unsubscribe callback (future)
            await self._unsubscribe()

    def subscribe(self, topic, qos=0) -> collections.abc.Awaitable:
        """Subscribe the client to a topic"""

        # Developers Notes:
        # This returns an awaitable object (`Subscribe`) that sets up the `Subscription`.

        client_wrapper = self
        return self.Subscribe(client_wrapper, topic=topic, qos=qos)


class Connect:
    """
    An async context manager that provides a connected MQTT Client.
    Responsible for setting up and tearing down the client.

    >>> async with connect('iot.eclipse.org') as client:
    >>>     await client.publish('test/message', 'hello world', qos=1)

    >>> client = await connect('iot.eclipse.org')
    >>> await client.publish('test/message', 'hello world', qos=1)
    """

    def __init__(
        self,
        broker_host: str,
        broker_port: int = 1883,
        client_id: Optional[str] = None,
        clean_session=True,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        receive_maximum: Optional[int] = None,
    ):

        self.loop = loop or asyncio.get_event_loop()

        client_args = {}
        if receive_maximum:
            client_args["receive_maximum"] = receive_maximum

        self.client = Client(client_id, clean_session=clean_session, **client_args)
        self.broker_host = broker_host
        self.broker_port = broker_port
        self._connect_future = self.loop.create_future()
        self._disconnect_future = self.loop.create_future()
        self._receive_maximum = receive_maximum

    async def _connect(self) -> MqttClientWrapper:
        def _on_connect(client, flags, rc, properties):
            self._connect_future.set_result(client)

        self.client.on_connect = _on_connect

        await self.client.connect(self.broker_host, self.broker_port)
        return await self._connect_future

    async def _disconnect(self):
        def _on_disconnect(client, packet, exc=None):
            self._disconnect_future.set_result(packet)

        self.client.on_disconnect = _on_disconnect
        await self.client.disconnect()
        return await self._disconnect_future

    def __await__(self):
        return self.__await_impl__().__await__()

    async def __await_impl__(self):
        client = await self._connect()
        return MqttClientWrapper(
            client, loop=self.loop, receive_maximum=self._receive_maximum
        )

    async def __aenter__(self) -> MqttClientWrapper:
        return await self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._disconnect()


# Make the context manager look like a function
connect = Connect
