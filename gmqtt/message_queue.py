import asyncio
import enum
import functools
import collections

# from .aioclient import Subscription
from .client import Message

from typing import List, Tuple, Dict, Callable


class TopicFilter:
    def __init__(self, topic_filter: str):
        self.levels = topic_filter.split("/")
        TopicFilter.validate_topic(self.levels)

    @staticmethod
    def validate_topic(filter_levels: List[str]):
        if filter_levels[-1][-1] == "#":
            if len(filter_levels[-1]) > 1:
                raise ValueError("Multi-level wildcard must be on its own level")

        if "#" in "".join(filter_levels[:-1]):
            raise ValueError(
                "Multi-level wildcard must be at the end of the topic filter"
            )

        for level in filter_levels:
            if len(level) > 1:
                if "+" in level:
                    raise ValueError("Single-level wildcard (+) only allowed by itself")

    def match(self, topic: str) -> bool:
        topic_levels = topic.split("/")

        for filter_level, topic_level in zip(self.levels, topic_levels):
            if filter_level == "+":
                continue
            elif filter_level == "#":
                return True
            else:
                if filter_level != topic_level:
                    return False
        return True

    def __hash__(self):
        return hash("/".join(self.levels))


class Subscription:
    def __init__(self, message_queue: asyncio.Queue, on_unsubscribe: Callable):
        self._incoming_messages = message_queue
        self._on_unsubscribe = on_unsubscribe

    async def recv(self):
        """Receive the next message published to this subscription"""
        message = await self._incoming_messages.get()
        # TODO: Hold off sending PUBACK for `message` until this point
        return message

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.recv()

    async def unsubscribe(self):
        await self._on_unsubscribe()

    def _add_message(self, message: Message):
        self._incoming_messages.put_nowait(message)


class DropPolicy(enum.Enum):
    OLDEST_FIRST = enum.auto()


class SubscriptionManager:
    """
    Manages incoming messages and the downstream subscription objects.

    Handles:
      - Maximum Queue size and message drop policy
      - Routing messages to subscriptions based on topic (possibily with wildcards)
    """

    def __init__(
        self, receive_maximum: int, drop_policy: DropPolicy = DropPolicy.OLDEST_FIRST
    ):
        self.receive_maximum = receive_maximum
        self.drop_policy = drop_policy
        self.subs: Dict[TopicFilter, List["asyncio.Queue"]] = collections.defaultdict(
            list
        )
        self.size = 0

    async def add_subscription(self, topic_filter_str: str) -> Subscription:
        topic_filter = TopicFilter(topic_filter_str)

        subscribed_messages = asyncio.Queue(self.receive_maximum)
        sub_id = (topic_filter, len(self.subs[topic_filter]))
        self.subs[topic_filter].append(subscribed_messages)

        return Subscription(
            message_queue=subscribed_messages,
            on_unsubscribe=functools.partial(self.remove_subscription, sub_id=sub_id),
        )

    async def remove_subscription(self, sub_id: Tuple[TopicFilter, int]):
        topic_filter, idx = sub_id

        # TODO: Unsubscribe on underlying client
        # Note: Don't remove so indexes are preserved
        self.subs[topic_filter][idx] = None

    def on_message(self, message: Message):
        # TODO: check self.size

        # if over, attempt to drop qos=0 packet using `drop_policy`

        # if under, add to appropiate queues:
        for subscription_topic in self.subs:
            if subscription_topic.match(message.topic):
                for subscription in self.subs[subscription_topic]:
                    subscription.put_nowait(message)
