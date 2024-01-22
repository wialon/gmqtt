import pytest
from unittest.mock import MagicMock

import gmqtt
from gmqtt import aioclient, message_queue
from gmqtt.message_queue import TopicFilter
from gmqtt.aioclient import MqttClientWrapper
from gmqtt.client import Message

# TODO: Fixtures
# Mock client


@pytest.mark.asyncio
async def test_plain_subscription():
    sm = message_queue.SubscriptionManager(999)
    sub = await sm.add_subscription("topic/TEST")

    message = gmqtt.Message(topic="topic/TEST", payload="payload")
    sm.on_message(message)

    received = await sub.recv()
    assert received.topic == "topic/TEST"


@pytest.mark.asyncio
async def test_wildcard_subscription():
    sm = message_queue.SubscriptionManager(999)
    sub = await sm.add_subscription("topic/+")

    message = gmqtt.Message(topic="topic/TEST", payload="payload")
    sm.on_message(message)

    received = await sub.recv()
    assert received.topic == "topic/TEST"


def test_match_topic_filter():
    tf = TopicFilter("topic/TEST")
    assert tf.match("topic/TEST")
    assert tf.match("topic/FOO") == False


def test_match_topic_filter_multilevel_wildcard():
    tf = TopicFilter("topic/#")

    assert tf.match("topic/TEST/1")
    assert tf.match("topic/1")
    assert tf.match("topic/")
    assert tf.match("topic")


def test_invalid_multilevel_wildcard():

    with pytest.raises(ValueError) as exc:
        TopicFilter("sport/tennis/#/ranking")

    with pytest.raises(ValueError):
        TopicFilter("#/tailing")

    with pytest.raises(ValueError):
        TopicFilter("sport/tennis#")


@pytest.fixture
def client():
    mock_inner = MagicMock()
    mock_inner.subscribe.return_value = None
    mock_inner.on_message = None

    wrapper_client = MqttClientWrapper(mock_inner)

    return wrapper_client, mock_inner

@pytest.mark.asyncio
async def test_multiple_subs_duplicate_messages(client):
    client, mocked_inner = client

    sub1 = await client.subscribe("test/test")
    sub2 = await client.subscribe("test/test")

    message = Message(topic="test/test", payload="payload")
    mocked_inner.on_message(client=mocked_inner, topic=message.topic, payload=message.payload, qos=message.qos, properties={})

    msg1 = await sub1.recv()
    msg2 = await sub2.recv()

    assert msg1.topic == "test/test"
    assert msg2.topic == "test/test"
    assert msg1.payload == b"payload"
    assert msg2.payload == b"payload"
