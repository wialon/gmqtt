
from typing import Litteral

DropPolicy = Litteral["oldest_first"]

class SubscriptionManager:
    """
    Manages incoming messages and the downstream subscription objects.

    Handles:
      - Maximum Queue size and message drop policy
      - Routing messages to subscriptions based on topic (possibily with wildcards)
    """


    def __init__(self, recieve_maximum: int, drop_policy: DropPolicy="oldest_first"):
        self.subs = collections.defaultdict(list)

        self.receive_maximum = receive_maximum
        self.size = 0

        self.drop_policy = drop_policy


    async def add_subscription(topic: str) -> Subscription:
        subscribed_messages = asyncio.Queue(receive_maximum)
        self.subs[subscription_topic].append(subscribed_messages)

        # TODO: see `unsubscribe`
        return Subscription(message_queue=subscribed_messages, 
                on_unsubscribe=functools.partial(self.unsubscribe, topic=topic))

    async def remove_subscription(self, topic: str):
        # TODO: Need to track invidual subscribers, so if multiple subs are on a topic,
        # the correct one is unsubscribed.
        pass


    async def on_message(self, message):

        # check self.size


        # if over, attempt to drop qos=0 packet using `drop_policy`

        # if under, add to appropiate queues:
        for subscription_topic in self.subs:
            if match(subscription_topic, message.topic):
                for subscription in self.subs[subscription_topic]:
                    subscription.on_message(
            if message.




