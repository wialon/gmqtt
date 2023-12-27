import asyncio
import logging
import traceback

from gmqtt.threaded.queue import SizeLimitedQueue
from gmqtt.threaded.thread import MQTTThread
from gmqtt.threaded.utils import PushType, PopType, MAX_QUEUE_SIZE

logger = logging.getLogger(__name__)

class ThreadedClient:
    def __init__(self, client_id, username, password, host, port=8883, ssl=True, max_queue_size=MAX_QUEUE_SIZE):
        self.push_queue = SizeLimitedQueue()
        self.pop_queue = SizeLimitedQueue()

        self._is_connected = False

        self.client_id = client_id
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.ssl = ssl

        self.max_queue_size = max_queue_size

        self.thread = MQTTThread(client_id, username, password, self.push_queue, self.pop_queue, host, port, ssl, max_queue_size=self.max_queue_size)

    @property
    def is_connected(self):
        return self._is_connected

    def publish(self,*args, **kwargs):
        try:
            self.push_queue.put((PushType.PUBLISH, args, kwargs))
        except BrokenPipeError:
            pass
        except EOFError:
            pass

    def subscribe(self, *args, **kwargs):
        try:
            self.push_queue.put((PushType.SUBSCRIBE, args, kwargs))
        except BrokenPipeError:
            pass
        except EOFError:
            pass

    def on_connect(self, client, flags, rc, properties):
        pass

    def on_message(self, client, topic, payload, qos, properties):
        pass

    def on_disconnect(self, client, packet, exc=None):
        pass

    def on_subscribe(self, client, mid, qos, properties):
        pass

    async def _auto_pop(self):
        while True:
            try:
                if self.pop_queue.qsize() > 0:
                    pop_type, args, kwargs = self.pop_queue.get_nowait()

                    if pop_type == PopType.MESSAGE:
                        self.on_message(None, *args, **kwargs)
                    elif pop_type == PopType.CONNECT:
                        self._is_connected = True
                        self.on_connect(None, *args, **kwargs)
                    elif pop_type == PopType.SUBSCRIBE:
                        self.on_subscribe(None, *args, **kwargs)
                    elif pop_type == PopType.DISCONNECT:
                        self._is_connected = False
                        self.on_disconnect(None, *args, **kwargs)
                else:
                    await asyncio.sleep(0.001)

            except EOFError:
                logging.error("QUEUE CLOSED - EXITING")
                break
            except Exception:
                traceback.print_exc()
                await asyncio.sleep(60)

    async def run(self):
        self.thread.start()
        await self._auto_pop()

    def shutdown(self):
        self.thread.shutdown()
