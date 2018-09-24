import time

import asyncio

import gmqtt
import logging


class Callbacks:

    def __init__(self):
        self.messages = []
        self.publisheds = []
        self.subscribeds = []
        self.connack = None

        self.disconnected = False
        self.connected = False

    def __str__(self):
        return str(self.messages) + str(self.messagedicts) + str(self.publisheds) + \
               str(self.subscribeds) + str(self.unsubscribeds) + str(self.disconnects)

    def clear(self):
        self.__init__()

    def on_disconnect(self, client, packet):
        logging.info('[DISCONNECTED {}]'.format(client._client_id))
        self.disconnected = True

    def on_message(self, client, topic, payload, qos, properties):
        logging.info('[RECV MSG {}] TOPIC: {} PAYLOAD: {} QOS: {} PROPERTIES: {}'
                     .format(client._client_id, topic, payload, qos, properties))
        self.messages.append((topic, payload, qos, properties))

    def on_subscribe(self, client, mid, qos):
        logging.info('[SUBSCRIBED {}] QOS: {}'.format(client._client_id, qos))
        self.subscribeds.append(mid)

    def on_connect(self, client, flags, rc, properties):
        logging.info('[CONNECTED {}]'.format(client._client_id))
        self.connected = True
        self.connack = (flags, rc, properties)

    def register_for_client(self, client):
        client.on_disconnect = self.on_disconnect
        client.on_message = self.on_message
        client.on_connect = self.on_connect
        client.on_subscribe = self.on_subscribe


async def clean_retained(host, port, username, password=None):
    def on_message(client, topic, payload, qos, properties):
        curclient.publish(topic, b"", qos=0, retain=True)

    curclient = gmqtt.Client("clean retained".encode("utf-8"), clean_session=True)

    curclient.set_auth_credentials(username, password)
    curclient.on_message = on_message
    await curclient.connect(host=host, port=port)
    curclient.subscribe("#")
    await asyncio.sleep(10)  # wait for all retained messages to arrive
    await curclient.disconnect()
    time.sleep(.1)


async def cleanup(host, port=1883, username=None, password=None, client_ids=None):
    # clean all client state
    print("clean up starting")
    client_ids = client_ids or ("myclientid", "myclientid2", "myclientid3")

    for clientid in client_ids:
        curclient = gmqtt.Client(clientid.encode("utf-8"), clean_session=True)
        curclient.set_auth_credentials(username=username, password=password)
        await curclient.connect(host=host, port=port)
        time.sleep(.1)
        await curclient.disconnect()
        time.sleep(.1)

    # clean retained messages
    await clean_retained(host, port, username, password=password)
    print("clean up finished")
