import pytest

import gmqtt
from gmqtt import aioclient, message_queue
from gmqtt.message_queue import TopicFilter

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
