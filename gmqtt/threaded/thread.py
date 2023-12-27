import asyncio
import logging
import traceback
from threading import Thread

from gmqtt import Client
from gmqtt.threaded.utils import MAX_QUEUE_SIZE, PopType, PushType

logger = logging.getLogger(__name__)

class MQTTThread(Thread):

    def __init__(self, client_id, push_queue, pop_queue):
        super().__init__(target=self._start_thread)

        self.push_queue = push_queue
        self.pop_queue = pop_queue

        self.client_id = client_id

        self.loop = asyncio.new_event_loop()
        self.client = None

        self._stop_event = asyncio.Event()

    def publish(self, *args, **kwargs):
        self.client.publish(*args, **kwargs)

    def _push_queue(self, msg_type, args, kwargs):
        try:
            self.pop_queue.put((msg_type, args[1:], kwargs))
        except BrokenPipeError:
            pass
        except EOFError:
            pass

    def on_connect(self, client, flags, rc, properties):
        logger.info("MQTT CONNECTED")
        self._push_queue(PopType.CONNECT, [], {
            flags: flags,
            rc: rc,
            properties: properties
        })


    def on_message(self, client, topic, payload, qos, properties):
        self._push_queue(PopType.MESSAGE, [], {
            topic: topic,
            payload: payload,
            qos: qos,
            properties: properties
        })

    def on_disconnect(self, client, packet, exc=None):
        logger.info("MQTT DISCONNECTED")
        self._push_queue(PopType.DISCONNECT, [], {
            packet: packet,
            exc: exc
        })


    def on_subscribe(self, client, mid, qos, properties):
        self._push_queue(PopType.SUBSCRIBE, [], {
            mid: mid,
            qos: qos,
            properties: properties
        })

    async def _auto_push(self):
        while not self._stop_event.is_set():
            try:
                if self.push_queue.qsize() > 0:
                    push_type, args, kwargs = self.push_queue.get_nowait()

                    if push_type == PushType.PUBLISH:
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


    async def _serve(self):
        publish_task = asyncio.ensure_future(self._auto_push())

        await self._stop_event.wait()

    async def init(self):
        self.client = Client(self.client_id)

        self.client.on_message = self.on_message
        self.client.on_connect = self.on_connect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_disconnect = self.on_disconnect

        return self.client

    def _start_thread(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._serve())