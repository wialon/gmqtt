import asyncio
import os
import time

import pytest

import gmqtt
from tests.utils import Callbacks, cleanup, clean_retained

host = 'mqtt.flespi.io'
port = 1883
username = os.getenv('USERNAME', 'fake_token')

PREFIX = str(time.time()) + '/'

TOPICS = (PREFIX + "TopicA", PREFIX + "TopicA/B", PREFIX + "TopicA/C", PREFIX + "TopicA/D", PREFIX + "/TopicA")
WILDTOPICS = (PREFIX + "TopicA/+", PREFIX + "+/C", PREFIX + "#", PREFIX + "/#", PREFIX + "/+", PREFIX + "+/+", PREFIX + "TopicA/#")
NOSUBSCRIBE_TOPICS = (PREFIX + "test/nosubscribe",)


@pytest.fixture()
async def init_clients():
    await cleanup(host, port, username, prefix=PREFIX)

    aclient = gmqtt.Client(PREFIX + "myclientid", clean_session=True)
    aclient.set_auth_credentials(username)
    callback = Callbacks()
    callback.register_for_client(aclient)

    bclient = gmqtt.Client(PREFIX + "myclientid2", clean_session=True)
    bclient.set_auth_credentials(username)
    callback2 = Callbacks()
    callback2.register_for_client(bclient)

    yield aclient, callback, bclient, callback2

    await aclient.disconnect()
    await bclient.disconnect()


@pytest.mark.asyncio
async def test_basic(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    await aclient.connect(host=host, port=port, version=4)
    await bclient.connect(host=host, port=port, version=4)
    bclient.subscribe(TOPICS[0])
    await asyncio.sleep(1)

    aclient.publish(TOPICS[0], b"qos 0")
    aclient.publish(TOPICS[0], b"qos 1", 1)
    aclient.publish(TOPICS[0], b"qos 2", 2)
    await asyncio.sleep(1)
    assert len(callback2.messages) == 3


@pytest.mark.asyncio
async def test_basic_subscriptions(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    await aclient.connect(host=host, port=port)
    await bclient.connect(host=host, port=port)

    subscriptions = [
        gmqtt.Subscription(TOPICS[1], qos=1),
        gmqtt.Subscription(TOPICS[2], qos=2),
    ]
    bclient.subscribe(subscriptions, user_property=('key', 'value'), subscription_identifier=1)

    bclient.subscribe(gmqtt.Subscription(TOPICS[3], qos=1), user_property=('key', 'value'), subscription_identifier=2)
    await asyncio.sleep(1)

    aclient.publish(TOPICS[3], b"qos 0")
    aclient.publish(TOPICS[1], b"qos 1", 1)
    aclient.publish(TOPICS[2], b"qos 2", 2)
    await asyncio.sleep(1)
    assert len(callback2.messages) == 3


@pytest.mark.asyncio
async def test_retained_message(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    await aclient.connect(host=host, port=port)
    aclient.publish(TOPICS[1], b"ret qos 0", 0, retain=True, user_property=("a", "2"))
    aclient.publish(TOPICS[2], b"ret qos 1", 1, retain=True, user_property=("c", "3"))
    aclient.publish(TOPICS[3], b"ret qos 2", 2, retain=True, user_property=(("a", "2"), ("c", "3")))

    await asyncio.sleep(2)

    await bclient.connect(host=host, port=port)
    bclient.subscribe(WILDTOPICS[0], qos=2)
    await asyncio.sleep(1)

    assert len(callback2.messages) == 3
    for msg in callback2.messages:
        assert msg[3]['retain'] == True

    aclient.publish(TOPICS[2], b"ret qos 1", 1, retain=True, user_property=("c", "3"))
    await asyncio.sleep(1)

    assert len(callback2.messages) == 4
    assert callback2.messages[3][3]['retain'] == 0

    await clean_retained(host, port, username, prefix=PREFIX)


@pytest.mark.asyncio
async def test_will_message(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    # re-initialize aclient with will message
    will_message = gmqtt.Message(TOPICS[2], "I'm dead finally")
    aclient = gmqtt.Client(PREFIX + "myclientid3", clean_session=True, will_message=will_message)
    aclient.set_auth_credentials(username)

    await aclient.connect(host, port=port)

    await bclient.connect(host=host, port=port)
    bclient.subscribe(TOPICS[2])

    await asyncio.sleep(1)
    await aclient.disconnect(reason_code=4)
    await asyncio.sleep(1)
    assert len(callback2.messages) == 1


@pytest.mark.asyncio
async def test_no_will_message_on_gentle_disconnect(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    # re-initialize aclient with will message
    will_message = gmqtt.Message(TOPICS[2], "I'm dead finally")
    aclient = gmqtt.Client(PREFIX + "myclientid3", clean_session=True, will_message=will_message)
    aclient.set_auth_credentials(username)

    await aclient.connect(host, port=port)

    await bclient.connect(host=host, port=port)
    bclient.subscribe(TOPICS[2])

    await asyncio.sleep(1)
    await aclient.disconnect(reason_code=0)
    await asyncio.sleep(1)
    assert len(callback2.messages) == 0


@pytest.mark.asyncio
async def test_shared_subscriptions(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    shared_sub_topic = '$share/sharename/{}x'.format(PREFIX)
    shared_pub_topic = PREFIX + 'x'

    await aclient.connect(host=host, port=port)
    aclient.subscribe(shared_sub_topic)
    aclient.subscribe(TOPICS[0])

    await   bclient.connect(host=host, port=port)
    bclient.subscribe(shared_sub_topic)
    bclient.subscribe(TOPICS[0])

    pubclient = gmqtt.Client(PREFIX + "myclient3", clean_session=True)
    pubclient.set_auth_credentials(username)
    await pubclient.connect(host, port)

    count = 10
    for i in range(count):
        pubclient.publish(TOPICS[0], "message " + str(i), 0)
    j = 0
    while len(callback.messages) + len(callback2.messages) < 2 * count and j < 20:
        await asyncio.sleep(1)
        j += 1
    await asyncio.sleep(1)
    assert len(callback.messages) == count
    assert len(callback2.messages) == count

    callback.clear()
    callback2.clear()

    count = 10
    for i in range(count):
        pubclient.publish(shared_pub_topic, "message " + str(i), 0)
    j = 0
    while len(callback.messages) + len(callback2.messages) < count and j < 20:
        await asyncio.sleep(1)
        j += 1
    await asyncio.sleep(1)
    # Each message should only be received once
    assert len(callback.messages) + len(callback2.messages) == count
    assert len(callback.messages) > 0
    assert len(callback2.messages) > 0


@pytest.mark.asyncio
async def test_assigned_clientid():
    noidclient = gmqtt.Client("", clean_session=True)
    noidclient.set_auth_credentials(username)
    callback = Callbacks()
    callback.register_for_client(noidclient)
    await noidclient.connect(host=host, port=port)
    await noidclient.disconnect()
    assert callback.connack[2]['assigned_client_identifier'][0] != ""


@pytest.mark.asyncio
async def test_unsubscribe(init_clients):
    aclient, callback, bclient, callback2 = init_clients
    await bclient.connect(host=host, port=port)
    await aclient.connect(host=host, port=port)

    bclient.subscribe(TOPICS[1])
    bclient.subscribe(TOPICS[2])
    bclient.subscribe(TOPICS[3])
    await asyncio.sleep(1)

    aclient.publish(TOPICS[1], b"topic 0 - subscribed", 1, retain=False)
    aclient.publish(TOPICS[2], b"topic 1", 1, retain=False)
    aclient.publish(TOPICS[3], b"topic 2", 1, retain=False)
    await asyncio.sleep(1)
    assert len(callback2.messages) == 3
    callback2.clear()
    # Unsubscribe from one topic
    bclient.unsubscribe(TOPICS[1])
    await asyncio.sleep(3)

    aclient.publish(TOPICS[1], b"topic 0 - unsubscribed", 1, retain=False)
    aclient.publish(TOPICS[2], b"topic 1", 1, retain=False)
    aclient.publish(TOPICS[3], b"topic 2", 1, retain=False)
    await asyncio.sleep(2)

    assert len(callback2.messages) == 2


@pytest.mark.asyncio
async def test_overlapping_subscriptions(init_clients):
    aclient, callback, bclient, callback2 = init_clients
    await bclient.connect(host=host, port=port)
    await aclient.connect(host=host, port=port)

    aclient.subscribe(TOPICS[3], qos=2, subscription_identifier=21)
    aclient.subscribe(WILDTOPICS[6], qos=1, subscription_identifier=42)
    await asyncio.sleep(1)
    bclient.publish(TOPICS[3], b"overlapping topic filters", 2)
    await asyncio.sleep(1)
    assert len(callback.messages) in [1, 2]
    if len(callback.messages) == 1:
        assert callback.messages[0][2] == 2
        assert set(callback.messages[0][3]['subscription_identifier']) == {42, 21}
    else:
        assert (callback.messages[0][2] == 2 and callback.messages[1][2] == 1) or \
               (callback.messages[0][2] == 1 and callback.messages[1][2] == 2)


@pytest.mark.asyncio
async def test_redelivery_on_reconnect(init_clients):
    # redelivery on reconnect. When a QoS 1 or 2 exchange has not been completed, the server should retry the
    # appropriate MQTT packets
    messages = []

    def on_message(client, topic, payload, qos, properties):
        print('MSG', (topic, payload, qos, properties))
        messages.append((topic, payload, qos, properties))
        return 131

    aclient, callback, bclient, callback2 = init_clients

    disconnect_client = gmqtt.Client(PREFIX + 'myclientid3', optimistic_acknowledgement=False,
                                     clean_session=False, session_expiry_interval=99999)
    disconnect_client.set_config({'reconnect_retries': 0})
    disconnect_client.on_message = on_message
    disconnect_client.set_auth_credentials(username)

    await disconnect_client.connect(host=host, port=port)
    disconnect_client.subscribe(WILDTOPICS[6], 2)

    await asyncio.sleep(1)
    await aclient.connect(host, port)
    await asyncio.sleep(1)

    aclient.publish(TOPICS[1], b"", 1, retain=False)
    aclient.publish(TOPICS[3], b"", 2, retain=False)
    await asyncio.sleep(1)
    messages = []
    await disconnect_client.reconnect()

    await asyncio.sleep(2)
    assert len(messages) == 2
    await disconnect_client.disconnect()


@pytest.mark.asyncio
async def test_async_on_message(init_clients):
    # redelivery on reconnect. When a QoS 1 or 2 exchange has not been completed, the server should retry the
    # appropriate MQTT packets
    messages = []

    async def on_message(client, topic, payload, qos, properties):
        print('MSG', (topic, payload, qos, properties))
        await asyncio.sleep(0.5)
        messages.append((topic, payload, qos, properties))
        return 131

    aclient, callback, bclient, callback2 = init_clients

    disconnect_client = gmqtt.Client(PREFIX + 'myclientid3', optimistic_acknowledgement=False,
                                     clean_session=False, session_expiry_interval=99999)
    disconnect_client.set_config({'reconnect_retries': 0})
    disconnect_client.on_message = on_message
    disconnect_client.set_auth_credentials(username)

    await disconnect_client.connect(host=host, port=port)
    disconnect_client.subscribe(WILDTOPICS[6], 2)

    await asyncio.sleep(1)
    await aclient.connect(host, port)
    await asyncio.sleep(1)

    aclient.publish(TOPICS[1], b"", 1, retain=False)
    aclient.publish(TOPICS[3], b"", 2, retain=False)
    await asyncio.sleep(2)
    messages = []
    await disconnect_client.reconnect()

    await asyncio.sleep(2)
    assert len(messages) == 2
    await disconnect_client.disconnect()


@pytest.mark.asyncio
async def test_request_response(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    await aclient.connect(host=host, port=port)
    await bclient.connect(host=host, port=port)

    aclient.subscribe(WILDTOPICS[0], 2)

    bclient.subscribe(WILDTOPICS[0], 2)

    await asyncio.sleep(1)
    # client a is the requester
    aclient.publish(TOPICS[1], b"request", 1, response_topic=TOPICS[2], correlation_data=b'334')

    await asyncio.sleep(1)

    # client b is the responder
    assert len(callback2.messages) == 1

    assert callback2.messages[0][3]['response_topic'] == [TOPICS[2], ]
    assert callback2.messages[0][3]['correlation_data'] == [b"334", ]

    bclient.publish(callback2.messages[0][3]['response_topic'][0], b"response", 1,
                    correlation_data=callback2.messages[0][3]['correlation_data'][0])

    await asyncio.sleep(1)
    assert len(callback.messages) == 2


@pytest.mark.asyncio
async def test_subscribe_no_local(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    await aclient.connect(host=host, port=port)
    await bclient.connect(host=host, port=port)

    aclient.subscribe(WILDTOPICS[0], 2, no_local=True)

    bclient.subscribe(WILDTOPICS[0], 2)

    await asyncio.sleep(1)

    aclient.publish(TOPICS[1], b"aclient msg", 1)

    bclient.publish(TOPICS[1], b"bclient msg", 1)

    await asyncio.sleep(1)

    assert len(callback.messages) == 1
    assert len(callback2.messages) == 2


@pytest.mark.asyncio
async def test_subscribe_retain_01_handling_flag(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    await aclient.connect(host=host, port=port)
    await bclient.connect(host=host, port=port)

    aclient.publish(TOPICS[1], b"ret qos 1", 1, retain=True)

    await asyncio.sleep(1)

    bclient.subscribe(WILDTOPICS[0], qos=2, retain_handling_options=0)

    await asyncio.sleep(1)

    assert len(callback2.messages) == 1

    bclient.subscribe(WILDTOPICS[0], qos=2, retain_handling_options=0)

    await asyncio.sleep(1)

    assert len(callback2.messages) == 2

    bclient.subscribe(WILDTOPICS[0], qos=2, retain_handling_options=1)

    await asyncio.sleep(1)

    assert len(callback2.messages) == 2


@pytest.mark.asyncio
async def test_subscribe_retain_2_handling_flag(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    await aclient.connect(host=host, port=port)
    await bclient.connect(host=host, port=port)

    aclient.publish(TOPICS[1], b"ret qos 1", 1, retain=True)

    await asyncio.sleep(1)

    bclient.subscribe(WILDTOPICS[0], qos=2, retain_handling_options=2)

    await asyncio.sleep(1)

    assert len(callback2.messages) == 0


@pytest.mark.asyncio
async def test_basic_ssl(init_clients):
    aclient, callback, bclient, callback2 = init_clients
    ssl_port = 8883

    await aclient.connect(host=host, port=ssl_port, ssl=True, version=4)
    await bclient.connect(host=host, port=ssl_port, ssl=True, version=5)
    bclient.subscribe(TOPICS[0])
    await asyncio.sleep(1)

    aclient.publish(TOPICS[0], b"qos 0")
    aclient.publish(TOPICS[0], b"qos 1", 1)
    aclient.publish(TOPICS[0], b"qos 2", 2)
    await asyncio.sleep(1)
    assert len(callback2.messages) == 3
