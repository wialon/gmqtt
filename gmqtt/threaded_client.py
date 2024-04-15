import asyncio
import logging
import traceback

from gmqtt.mqtt.handler import EventCallback
from gmqtt.threaded.queue import SizeLimitedQueue
from gmqtt.threaded.thread import MQTTThread
from gmqtt.threaded.utils import PushType, PopType

logger = logging.getLogger(__name__)

class ThreadedClient(EventCallback):
    def __init__(self, client_id):
        super().__init__()

        self.push_queue = SizeLimitedQueue()
        self.pop_queue = SizeLimitedQueue()

        #self._is_connected = False

        self.client_id = client_id

        self.loop = asyncio.get_event_loop()
        self.thread = MQTTThread(client_id, self.push_queue, self.pop_queue)

        self._stop_event = asyncio.Event()
        self._serve()

    def _serve(self):
        self.thread.start()

        with self.thread.start_up_lock.acquire(): # wait until thread has initialized
            asyncio.run_coroutine_threadsafe(self.thread.init(), self.thread.loop).result()

        self._pop_task = asyncio.ensure_future(self._auto_pop())

    def __del__(self):
        self.shutdown()

    @property
    def failed_connections(self):
        return self.thread.client.failed_connections if not self.thread is None else 0

    @property
    def connection_attempts(self):
        return self.thread.client.connection_attempts if not self.thread is None else 0

    @failed_connections.setter
    def failed_connections(self, v):
        pass

    @property
    def is_connected(self):
        return self.thread.client.is_connected if not self.thread.client is None else False

    def _push_message(self, msg_type, args, kwargs):
        try:
            self.push_queue.put((msg_type, args, kwargs))
        except BrokenPipeError:
            pass
        except EOFError:
            pass

    async def connect(self, *args, **kwargs):
        fut = asyncio.run_coroutine_threadsafe(self.thread.client.connect(*args, **kwargs), self.thread.loop)
        await asyncio.wrap_future(fut)

    def publish(self,*args, **kwargs):
        self._push_message(PushType.PUBLISH, args, kwargs)

    async def _subscribe_async(self, *args, **kwargs):
        return self.thread.client.subscribe(*args, **kwargs)
    def subscribe(self, *args, **kwargs):
        fut = asyncio.run_coroutine_threadsafe(self._subscribe_async(*args, **kwargs), self.thread.loop)
        return fut.result()

    async def _set_auth_credentials_async(self, username, password):
        return self.thread.client.set_auth_credentials(username, password)

    def set_auth_credentials(self, username, password):
        fut = asyncio.run_coroutine_threadsafe(self._set_auth_credentials_async(username, password), self.thread.loop)
        return fut.result()

    async def _auto_pop(self):
        while not self._stop_event.is_set():
            try:
                if self.pop_queue.qsize() > 0:
                    pop_type, args, kwargs = self.pop_queue.get_nowait()

                    if pop_type == PopType.MESSAGE:
                        self.on_message(self.thread.client, *args, **kwargs)
                    elif pop_type == PopType.CONNECT:
                        #self._is_connected = True
                        self.on_connect(self.thread.client, *args, **kwargs)
                    elif pop_type == PopType.SUBSCRIBE:
                        self.on_subscribe(self.thread.client, *args, **kwargs)
                    elif pop_type == PopType.DISCONNECT:
                        #self._is_connected = False
                        self.on_disconnect(self.thread.client, *args, **kwargs)
                else:
                    await asyncio.sleep(0.001)

            except EOFError:
                logging.error("QUEUE CLOSED - EXITING")
                break
            except Exception:
                traceback.print_exc()
                await asyncio.sleep(60)

    def shutdown(self):
        self._stop_event.set()
        self.thread.shutdown()

if __name__ == "__main__":
    async def main():
        tc = ThreadedClient("test")
        tc.set_auth_credentials("server_data", "c2e9b52284a2ae1230264f6ea3f340f4")
        await tc.connect("mqtt.elion.be", 8883, ssl=True)

        #await asyncio.Future()
        #tc.shutdown()

    asyncio.run(main())
