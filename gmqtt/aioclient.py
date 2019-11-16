import asyncio
from .client import Client, Message
from typing import Optional, Tuple


class MqttClientProtocol:
    """
    Wraps the normal client in the async/await API
    """

    def __init__(
        self, inner_client: Client, loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop

        self.cbclient = inner_client
        self.message_queue = asyncio.Queue()

    async def publish(self, topic: str, message: Message, qos=0):
        """Publish a message to the MQTT broker"""
        self.cbclient.publish(topic, message, qos)

    async def subscribe(self, topic: str, qos) -> Tuple[str, bytes]:
        """Subscribe to a MQTT topic and wait for a single message."""

        def _on_message(client, topic, payload, qos, properties):
            self.message_queue.put_nowait((topic, payload))

        self.cbclient.on_message = _on_message

        self.cbclient.subscribe(topic, qos)
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
        *args,
        **kwargs
    ):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop

        # cbclient: callback client.
        self.cbclient = Client(client_id)
        self.broker_host = broker_host
        self._connect_future = self.loop.create_future()
        self._disconnect_future = self.loop.create_future()

    async def _connect(self, broker_host) -> MqttClientProtocol:
        def _on_connect(client, flags, rc, properties):
            self._connect_future.set_result(client)

        self.cbclient.on_connect = _on_connect

        await self.cbclient.connect(broker_host)
        return await self._connect_future

    async def _disconnect(self):
        def _on_disconnect(client, packet, exc=None):
            self._disconnect_future.set_result(packet)

        self.cbclient.on_disconnect = _on_disconnect
        await self.cbclient.disconnect()
        return await self._disconnect_future

    async def __aenter__(self) -> MqttClientProtocol:
        cbclient = await self._connect(self.broker_host)
        return MqttClientProtocol(cbclient, self.loop)

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._disconnect()


# Make context manager look like a function `connect`
connect = Connect
