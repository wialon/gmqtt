import asyncio
import collections
from typing import Optional, Tuple, Callable

from .client import Client, Message


class Subscription:
    def __init__(self, on_unsubscribe: Callable):
        self._incoming_messages = asyncio.Queue()

    async def receive(self):
        """Receive the next message published to this subscription"""
        message = await self._incoming_messages.get()
        # TODO: Hold off sending PUBACK for `message` until this point
        return message

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.receive()

    async def unsubscribe(self):
        await self._on_unsubscribe()

    def _add_message(self, message: Message):
        self._incoming_messages.put_nowait(message)


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
        self.message_queue = asyncio.Queue(maxsize=receive_maximum or 0)
        self._subscriptions = collections.defaultdict(set)
        self._init_client()

    def _init_client(self):
        """Set up client so messages are forwarded to registered subscriptions:"""

        def _on_message(client, topic, payload, qos, properties):
            """When message received: Forward message to subscriptions that match `topic`"""
            message = Message(topic, payload, qos=qos, **properties)

            # TODO: Match subscriptions with wildcards
            for sub in self._subscriptions[topic]:
                # TODO: Handle recieved message when queue full (drop a qos=0 packet)
                sub._add_message(message)

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
            subscription = Subscription(on_unsubscribe=self._unsubscribe)
            self.client_wrapper._subscriptions[self.topic].add(subscription)
            self.client_wrapper.client.subscribe(self.topic, self.qos)

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
        self._connect_future = self.loop.create_future()
        self._disconnect_future = self.loop.create_future()
        self._receive_maximum = receive_maximum

    async def _connect(self, broker_host) -> MqttClientWrapper:
        def _on_connect(client, flags, rc, properties):
            self._connect_future.set_result(client)

        self.client.on_connect = _on_connect

        await self.client.connect(broker_host)
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
        client = await self._connect(self.broker_host)
        return MqttClientWrapper(
            client, loop=self.loop, receive_maximum=self._receive_maximum
        )

    async def __aenter__(self) -> MqttClientWrapper:
        return await self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._disconnect()


# Make the context manager look like a function
connect = Connect
