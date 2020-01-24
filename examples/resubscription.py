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


def on_subscribe(client, mid, qos, properties):
    # in order to check if all the subscriptions were successful, we should first get all subscriptions with this
    # particular mid (from one subscription request)
    subscriptions = client.get_subscriptions_by_mid(mid)
    for subscription, granted_qos in zip(subscriptions, qos):
        # in case of bad suback code, we can resend  subscription
        if granted_qos >= gmqtt.constants.SubAckReasonCode.UNSPECIFIED_ERROR.value:
            logging.warning('[RETRYING SUB {}] mid {}, reason code: {}, properties {}'.format(
                            client._client_id, mid, granted_qos, properties))
            client.resubscribe(subscription)
        logging.info('[SUBSCRIBED {}] mid {}, QOS: {}, properties {}'.format(
            client._client_id, mid, granted_qos, properties))


def assign_callbacks_to_client(client):
    # helper function which sets up client's callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe


def ask_exit(*args):
    STOP.set()


async def main(broker_host, broker_port, token):
    # create client instance, kwargs (session expiry interval and maximum packet size)
    # will be send as properties in connect packet
    sub_client = gmqtt.Client("clientgonnasub")

    assign_callbacks_to_client(sub_client)
    sub_client.set_auth_credentials(token, None)
    await sub_client.connect(broker_host, broker_port)

    # two overlapping subscriptions with different subscription identifiers
    subscriptions = [
        gmqtt.Subscription('TEST/PROPS/#', qos=1),
        gmqtt.Subscription('TEST2/PROPS/#', qos=2),
    ]
    sub_client.subscribe(subscriptions, subscription_identifier=1)

    pub_client = gmqtt.Client("clientgonnapub")

    assign_callbacks_to_client(pub_client)
    pub_client.set_auth_credentials(token, None)
    await pub_client.connect(broker_host, broker_port)

    # this message received by sub_client will have two subscription identifiers
    pub_client.publish('TEST/PROPS/42', '42 is the answer', qos=1, content_type='utf-8',
                       message_expiry_interval=60, user_property=('time', str(time.time())))
    pub_client.publish('TEST2/PROPS/42', '42 is the answer', qos=1, content_type='utf-8',
                       message_expiry_interval=60, user_property=('time', str(time.time())))

    await STOP.wait()
    await pub_client.disconnect()
    await sub_client.disconnect(session_expiry_interval=0)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    logging.basicConfig(level=logging.INFO)

    host = os.environ.get('HOST', 'mqtt.flespi.io')
    port = 1883
    token = os.environ.get('TOKEN', 'fake token')

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)

    loop.run_until_complete(main(host, port, token))
