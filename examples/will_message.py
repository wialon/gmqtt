import logging
import os
import signal
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
    # this message will be published by broker after client disconnects with "bad" code after 10 sec
    will_message = gmqtt.Message('TEST/WILL/42', "I'm dead finally", will_delay_interval=10)
    will_client = gmqtt.Client("clientgonnadie", will_message=will_message)

    assign_callbacks_to_client(will_client)
    will_client.set_auth_credentials(token, None)
    await will_client.connect(broker_host, broker_port)

    another_client = gmqtt.Client("clientgonnalisten")

    assign_callbacks_to_client(another_client)
    another_client.set_auth_credentials(token, None)
    await another_client.connect(broker_host, broker_port)

    another_client.subscribe('TEST/#')

    # reason code 4 - Disconnect with Will Message
    await will_client.disconnect(reason_code=4, reason_string="Smth went wrong")

    await STOP.wait()
    await another_client.disconnect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    logging.basicConfig(level=logging.INFO)

    host = os.environ.get('HOST', 'mqtt.flespi.io')
    port = 1883
    token = os.environ.get('TOKEN', 'fake token')

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)

    loop.run_until_complete(main(host, port, token))
