import asyncio
from .client import Client, Message
from typing import Optional, Tuple


class MqttClientWrapper:
    """
    Wraps the callback-based client in a async/await API
    """

    def __init__(
        self,
        inner_client: Client,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        receive_maximum: Optional[int] = None,
    ):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop

        self.client = inner_client
        self.message_queue = asyncio.Queue(maxsize=receive_maximum or 0)

    async def publish(self, topic: str, message: Message, qos=0):
        """Publish a message to the MQTT topic"""
        self.client.publish(topic, message, qos)

    async def subscribe(self, topic: str, qos: int) -> Tuple[str, Message]:
        """Subscribe to messages on the MQTT topic"""

        def _on_message(client, topic, payload, qos, properties):
            # TODO: Handle recieved message when queue full (drop a qos=0 packet)
            message = Message(topic, payload, qos=qos, **properties)
            self.message_queue.put_nowait((topic, message))

        self.client.on_message = _on_message

        self.client.subscribe(topic, qos)

        # TODO: Hold off sending PUBACK until this point
        return await self.message_queue.get()


class Connect:
    """
    An async context manager that provides a connected MQTT Client.
    Responsible for setting up and tearing down the client.

    >>> async with Connect('iot.eclipse.org') as client:
    >>>     await client.publish('test/message', 'hello world', qos=1)
    >>>     message = await client.subscribe('test/message')
    """

    def __init__(
        self,
        client_id: str,
        broker_host: str,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        receive_maximum: Optional[int] = None,
    ):

        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop

        self.client = Client(client_id, receive_maximum=receive_maximum)
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

    async def __aenter__(self) -> MqttClientWrapper:
        client = await self._connect(self.broker_host)
        return MqttClientWrapper(
            client, self.loop, recieve_maximum=self._receive_maximum
        )

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._disconnect()


# Make the context manager look like a function
connect = Connect
