import asyncio

import os
import pytest

import gmqtt
from tests.utils import Callbacks, cleanup, clean_retained

host = 'mqtt.flespi.io'
port = 1883
username = os.getenv('USERNAME', 'fake_token')

TOPICS = ("TopicA", "TopicA/B", "TopicA/C", "TopicA/D", "/TopicA")
WILDTOPICS = ("TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#")
NOSUBSCRIBE_TOPICS = ("test/nosubscribe",)


@pytest.fixture()
async def init_clients():
    await cleanup(host, port, username)

    aclient = gmqtt.Client("myclientid", clean_session=True)
    aclient.set_auth_credentials(username)
    callback = Callbacks()
    callback.register_for_client(aclient)

    bclient = gmqtt.Client("myclientid2", clean_session=True)
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
async def test_retained_message(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    await aclient.connect(host=host, port=port)
    aclient.publish(TOPICS[1], b"ret qos 0", 0, retain=True, user_property=("a", "2"))
    aclient.publish(TOPICS[2], b"ret qos 1", 1, retain=True, user_property=("c", "3"))
    aclient.publish(TOPICS[3], b"ret qos 2", 2, retain=True, user_property=("a", "2"))

    await asyncio.sleep(1)
    await aclient.disconnect()
    await asyncio.sleep(1)

    await bclient.connect(host=host, port=port)
    bclient.subscribe(WILDTOPICS[0], qos=2)
    await asyncio.sleep(1)

    assert len(callback2.messages) == 3

    await clean_retained(host, port, username)


@pytest.mark.asyncio
async def test_will_message(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    # re-initialize aclient with will message
    will_message = gmqtt.Message(TOPICS[2], "I'm dead finally")
    aclient = gmqtt.Client("myclientid3", clean_session=True, will_message=will_message)
    aclient.set_auth_credentials(username)

    await aclient.connect(host, port=port)

    await bclient.connect(host=host, port=port)
    bclient.subscribe(TOPICS[2])

    await asyncio.sleep(1)
    await aclient.disconnect(reason_code=4)
    await asyncio.sleep(1)
    assert len(callback2.messages) == 1


@pytest.mark.asyncio
async def test_shared_subscriptions(init_clients):
    aclient, callback, bclient, callback2 = init_clients

    shared_sub_topic = '$share/sharename/x'
    shared_pub_topic = 'x'

    await aclient.connect(host=host, port=port)
    aclient.subscribe(shared_sub_topic)
    aclient.subscribe(TOPICS[0])

    await   bclient.connect(host=host, port=port)
    bclient.subscribe(shared_sub_topic)
    bclient.subscribe(TOPICS[0])

    pubclient = gmqtt.Client("myclient3", clean_session=True)
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
    print(callback2.messages)

    aclient.publish(TOPICS[1], b"topic 0 - subscribed", 1, retain=False)
    aclient.publish(TOPICS[2], b"topic 1", 1, retain=False)
    aclient.publish(TOPICS[3], b"topic 2", 1, retain=False)
    await asyncio.sleep(1)
    print(callback2.messages)
    assert len(callback2.messages) == 3
    callback2.clear()
    # Unsubscribe from one topic
    bclient.unsubscribe(TOPICS[1])
    await asyncio.sleep(3)

    aclient.publish(TOPICS[1], b"topic 0 - unsubscribed", 1, retain=False)
    aclient.publish(TOPICS[2], b"topic 1", 1, retain=False)
    aclient.publish(TOPICS[3], b"topic 2", 1, retain=False)
    await asyncio.sleep(1)

    assert len(callback2.messages) == 2


@pytest.mark.asyncio
async def test_overlapping_subscriptions(init_clients):
    aclient, callback, bclient, callback2 = init_clients
    await bclient.connect(host=host, port=port)
    await aclient.connect(host=host, port=port)

    aclient.subscribe(TOPICS[3], qos=2, subscription_identifier=21)
    aclient.subscribe(WILDTOPICS[6], qos=1, subscription_identifier=42)
    await asyncio.sleep(1)
    print(TOPICS[3], WILDTOPICS[6])
    bclient.publish(TOPICS[3], b"overlapping topic filters", 2)
    await asyncio.sleep(1)
    assert len(callback.messages) in [1, 2]
    if len(callback.messages) == 1:
        print("This server is publishing one message for all matching overlapping subscriptions, not one for each.")
        assert callback.messages[0][2] == 2
        assert set(callback.messages[0][3]['subscription_identifier']) == set([42, 21])
    else:
        print("This server is publishing one message per each matching overlapping subscription.")
        assert (callback.messages[0][2] == 2 and callback.messages[1][2] == 1) or \
               (callback.messages[0][2] == 1 and callback.messages[1][2] == 2)

