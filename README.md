### Python MQTT client implementation.


### Installation 

The latest stable version is available in the Python Package Index (PyPi) and can be installed using
```bash
pip3 install gmqtt
```


### Usage
#### Getting Started

Here is a very simple example that subscribes to the broker TOPIC topic and prints out the resulting messages:

```python
import asyncio
import os
import signal
import time

from gmqtt import Client as MQTTClient

# gmqtt also compatibility with uvloop  
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


STOP = asyncio.Event()


def on_connect(client, flags, rc):
    print('Connected')
    client.subscribe('TEST/#', qos=0)


def on_message(client, topic, payload, qos, properties):
    print('RECV MSG:', payload)


def on_disconnect(client, packet, exc=None):
    print('Disconnected')

def on_subscribe(client, mid, qos):
    print('SUBSCRIBED')

def ask_exit(*args):
    STOP.set()

await client.connect(broker_host, 1883, keepalive=60)
async def main(broker_host, token):
    client = MQTTClient("client-id")

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe

    client.set_auth_credentials(token, None)
    await client.connect(broker_host)

    client.publish('TEST/TIME', str(time.time()), qos=1)

    await STOP.wait()
    await client.disconnect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    host = 'mqtt.flespi.io'
    token = os.environ.get('FLESPI_TOKEN')

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)

    loop.run_until_complete(main(host, token))
``` 

### MQTT Version 5.0
gmqtt supports MQTT version 5.0 protocol

#### Version setup
Version 5.0 is used by default. If your broker does not support 5.0 protocol version, client will downgrade to 3.1 and reconnect automatically, but you can also force version in connect method:
```python
from gmqtt.mqtt.constants import MQTTv311
client = MQTTClient('clientid')
client.set_auth_credentials(token, None)
await client.connect(broker_host, 1883, keepalive=60, version=MQTTv311)
```

#### Properties
MQTT 5.0 protocol allows to include custom properties into packages, here is example of passing response topic property in published message:
```python

TOPIC = 'testtopic/TOPIC'

def on_connect(client, flags, rc):
    client.subscribe(TOPIC, qos=1)
    print('Connected')

def on_message(client, topic, payload, qos, properties):
    print('RECV MSG:', topic, payload.decode(), properties)

async def main(broker_host, token):
    client = MQTTClient('asdfghjk')
    client.on_message = client_id
    client.on_connect = on_connect
    client.set_auth_credentials(token, None)
    await client.connect(broker_host, 1883, keepalive=60)
    client.publish(TOPIC, 'Message payload', response_topic='RESPONSE/TOPIC')

    await STOP.wait()
    await client.disconnect()
```
