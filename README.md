[![Build Status](https://travis-ci.com/Lenka42/gmqtt.svg?branch=master)](https://travis-ci.com/Lenka42/gmqtt)
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


def on_connect(client, flags, rc, properties):
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
from gmqtt.constants import MQTTv311
client = MQTTClient('clientid')
client.set_auth_credentials(token, None)
await client.connect(broker_host, 1883, keepalive=60, version=MQTTv311)
```

#### Properties
MQTT 5.0 protocol allows to include custom properties into packages, here is example of passing response topic property in published message:
```python

TOPIC = 'testtopic/TOPIC'

def on_connect(client, flags, rc, properties):
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
##### Connect properties
Connect properties are passed to `Client` object as kwargs (later they are stored together with properties received from broker in `client.properties` field). See example below.
* `session_expiry_interval` - `int` Session expiry interval in seconds. If the Session Expiry Interval is absent the value 0 is used. If it is set to 0, or is absent, the Session ends when the Network Connection is closed. If the Session Expiry Interval is 0xFFFFFFFF (max possible value), the Session does not expire.
* `receive_maximum` - `int` The Client uses this value to limit the number of QoS 1 and QoS 2 publications that it is willing to process concurrently.
* `user_property` - `tuple(str, str)` This property may be used to provide additional diagnostic or other information (key-value pairs).
* `maximum_packet_size` - `int` The Client uses the Maximum Packet Size (in bytes) to inform the Server that it will not process packets exceeding this limit.

Example:
```
client = gmqtt.Client("lenkaklient", receive_maximum=24000, session_expiry_interval=60, user_property=('myid', '12345'))
```

##### Publish properties
This properties will be also sent in publish packet from broker, they will be passed to `on_message` callback.
* `message_expiry_interval` - `int` If present, the value is the lifetime of the Application Message in seconds.
* `content_type` - `unicode` UTF-8 Encoded String describing the content of the Application Message. The value of the Content Type is defined by the sending and receiving application.
* `user_property` - `tuple(str, str)`
* `subscription_identifier` - `int` (see subscribe properties) sent by broker

Example:
```
def on_message(client, topic, payload, qos, properties):
    # properties example here: {'content_type': ['json'], 'user_property': [('timestamp', '1524235334.881058')], 'message_expiry_interval': [60], 'subscription_identifier': [42, 64]}
    print('RECV MSG:', topic, payload, properties)

client.publish('TEST/TIME', str(time.time()), qos=1, retain=True, message_expiry_interval=60, content_type='json')
```

##### Subscribe properties
* `subscription_identifier` - `int` If the Client specified a Subscription Identifier for any of the overlapping subscriptions the Server MUST send those Subscription Identifiers in the message which is published as the result of the subscriptions.

### Other examples
Check [examples directory](examples) for more use cases.
