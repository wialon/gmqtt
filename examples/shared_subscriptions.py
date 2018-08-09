import logging
import os
import signal
import time

import uvloop
import asyncio
import gmqtt

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

STOP = asyncio.Event()


def on_connect(client, flags, rc, properties):
    logging.info('[CONNECTED {}]'.format(client._client_id))


def on_message(client, topic, payload, qos, properties):
    logging.info('[RECV MSG {}] TOPIC: {} PAYLOAD: {} QOS: {} PROPERTIES: {}'
                 .format(client._client_id, topic, payload, qos, properties))


def on_disconnect(client, packet, exc=None):
    logging.info('[DISCONNECTED {}]'.format(client._client_id))


def on_subscribe(client, mid, qos):
    logging.info('[SUBSCRIBED {}] QOS: {}'.format(client._client_id, qos))


def assign_callbacks_to_client(client):
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe


def ask_exit(*args):
    STOP.set()


async def main(broker_host, broker_port, token):
    # initiate first client subscribed to TEST/SHARED/# in group mytestgroup
    sub_clientA = gmqtt.Client("clientgonnasubA")

    assign_callbacks_to_client(sub_clientA)
    sub_clientA.set_auth_credentials(token, None)
    await sub_clientA.connect(broker_host, broker_port)

    sub_clientA.subscribe('$share/mytestgroup/TEST/SHARED/#')

    # initiate second client subscribed to TEST/SHARED/# in group mytestgroup
    sub_clientB = gmqtt.Client("clientgonnasubB")

    assign_callbacks_to_client(sub_clientB)
    sub_clientB.set_auth_credentials(token, None)
    await sub_clientB.connect(broker_host, broker_port)

    sub_clientB.subscribe('$share/mytestgroup/TEST/SHARED/#')

    # this client will publish messages to TEST/SHARED/... topics
    pub_client = gmqtt.Client("clientgonnapub")

    assign_callbacks_to_client(pub_client)
    pub_client.set_auth_credentials(token, None)
    await pub_client.connect(broker_host, broker_port)

    # some of this messages will be received by client sub_clientA,
    # and another part by client sub_clientB, approximately 50/50
    for i in range(100):
        pub_client.publish('TEST/SHARED/{}'.format(i), i, user_property=('time', str(time.time())))

    await STOP.wait()
    await pub_client.disconnect()
    await sub_clientA.disconnect(session_expiry_interval=0)
    await sub_clientB.disconnect(session_expiry_interval=0)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    logging.basicConfig(level=logging.INFO)

    host = os.environ.get('HOST', 'mqtt.flespi.io')
    port = 1883
    token = os.environ.get('TOKEN', 'fake token')

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)

    loop.run_until_complete(main(host, port, token))
