import asyncio
import logging
import traceback
from threading import Thread

from gmqtt import Client
from gmqtt.threaded.utils import MAX_QUEUE_SIZE, PopType, PushType

logger = logging.getLogger(__name__)

class MQTTThread(Thread):

    def __init__(self, client_id, username, password, push_queue, pop_queue, host="mqtt.elion.be", port=8883, ssl=True, max_queue_size=MAX_QUEUE_SIZE):
        super().__init__(target=self._start_thread)

        self.push_queue = push_queue
        self.pop_queue = pop_queue

        self.client_id = client_id
        self.username = username
        self.password = password
        self.host = host
        self.ssl = ssl
        self.port = port

        self.loop = None
        self.client = None

        self._stop_event = asyncio.Event()

        self.max_queue_size = max_queue_size

    def publish(self, *args, **kwargs):
        self.client.publish(*args, **kwargs)

    def subscribe(self, *args, **kwargs):
        self.client.subscribe(*args, **kwargs)

    def on_connect(self, *args, **kwargs):
        logger.info("MQTT CONNECTED")
        try:
            self.pop_queue.put((PopType.CONNECT, args[1:], kwargs))
        except BrokenPipeError:
            pass
        except EOFError:
            pass

    def on_message(self, *args, **kwargs):
        try:
            self.pop_queue.put((PopType.MESSAGE, args[1:], kwargs))
        except BrokenPipeError:
            pass
        except EOFError:
            pass

    def on_disconnect(self, *args, **kwargs):
        logger.info("MQTT DISCONNECTED")

        try:
            self.pop_queue.put((PopType.DISCONNECT, args[1:], kwargs))
        except BrokenPipeError:
            pass
        except EOFError:
            pass

    def on_subscribe(self, *args, **kwargs):
        try:
            self.pop_queue.put((PopType.SUBSCRIBE, args[1:], kwargs))
        except BrokenPipeError:
            pass
        except EOFError:
            pass

    async def _auto_push(self):
        while True:
            try:
                if self.push_queue.qsize() > 0:
                    push_type, args, kwargs = self.push_queue.get_nowait()

                    if push_type == PushType.SUBSCRIBE:
                        self.subscribe(*args, **kwargs)
                    elif push_type == PushType.PUBLISH:
                        self.publish(*args, **kwargs)

                else:
                    await asyncio.sleep(0.001)

            except EOFError:
                logging.error("QUEUE CLOSED - EXITING")
                break
            except Exception:
                logging.error(traceback.format_exc())
                await asyncio.sleep(60)

    def shutdown(self):
        self._stop_event.set()


    async def _start_mqtt_async(self):
        publish_task = self.loop.ensure_future(self._auto_push())

        while True:
            try:
                await self.client.connect(self.host, self.port, self.ssl)
                await self._stop_event.wait()
                await self.client.disconnect()
                break
            except Exception:
                await asyncio.sleep(10)

    def _initialize_client(self):
        client = Client(self.client_id)
        client.set_auth_credentials(username=self.username, password=self.password)

        client.on_message = self.on_message
        client.on_connect = self.on_connect
        client.on_subscribe = self.on_subscribe
        client.on_disconnect = self.on_disconnect

        return client

    def _start_thread(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.client = self._initialize_client()

        self.loop.run_until_complete(self._start_mqtt_async())