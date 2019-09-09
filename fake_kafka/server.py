

from itertools import cycle
from collections import defaultdict
from typing import Dict, List, Tuple, Union, Any
import asyncio
import time
import logging

from .exceptions import NoAvailablePartition
from .messages import FakeKafkaMessage, FakeKafkaOffsetMessage

logger = logging.getLogger(__name__)


class _Unknown:
    pass


Unknown = _Unknown()


class _Alive:
    pass


Alive = _Alive()


class _Dead:
    pass


Dead = _Dead()

State = Union[_Unknown, _Alive, _Dead]


# Singleton FakeKafkaServer
class FakeKafkaServer:

    __instance: Any = None
    topics: Dict[str, Dict[int, List[FakeKafkaOffsetMessage]]]
    partitions: Dict[str, List[int]]
    consumers_state: Dict[str, Tuple[State, float]]
    topics_to_consumers: Dict[Tuple[str, str], List[str]]
    topics_to_groups: Dict[str, List[str]]
    consumers_to_topics: Dict[str, List[str]]
    consumers_to_groups: Dict[str, str]
    consumers_to_partitions: Dict[Tuple[str, str, str], List[int]]
    consumer_queues: Dict[Tuple[str, str], asyncio.Queue]
    partition_offsets: Dict[Tuple[str, int, str], int]

    def __new__(cls) -> 'FakeKafkaServer':
        if FakeKafkaServer.__instance is None:
            FakeKafkaServer.__instance = object.__new__(cls)
            FakeKafkaServer._init_topics()
        return FakeKafkaServer.__instance

    @classmethod
    def _init_topics(cls, partitions_per_topic: int = 1) -> None:
        cls.__instance.topics = defaultdict(cls.__instance.build_partitions)
        cls.__instance.partitions = defaultdict(lambda: list(range(partitions_per_topic)))
        cls.__instance.consumers_state = defaultdict(lambda: (Unknown, 0.0))
        cls.__instance.topics_to_consumers = defaultdict(list)
        cls.__instance.topics_to_groups = defaultdict(list)
        cls.__instance.consumers_to_topics = defaultdict(list)
        cls.__instance.consumers_to_groups = defaultdict(lambda: '')
        cls.__instance.consumers_to_partitions = defaultdict(list)
        cls.__instance.consumer_queues = dict()
        cls.__instance.partition_offsets = defaultdict(lambda: 0)

    def build_partitions(self) -> Dict[int, List[FakeKafkaOffsetMessage]]:
        return defaultdict(list)

    async def all_partitions(self, topic: str) -> List[int]:
        return self.partitions[topic]

    async def send(self, topic: str, message: FakeKafkaMessage) -> None:
        message2 = FakeKafkaOffsetMessage(topic=message.topic,
                                          partition=message.partition,
                                          offset=len(self.topics[topic][message.partition]),
                                          key=message.key,
                                          value=message.value,
                                          timestamp=message.timestamp)
        self.topics[topic][message2.partition].append(message2)

        for group in self.topics_to_groups[topic]:
            for consumer in self.topics_to_consumers[(topic, group)]:
                logger.debug('topic %s consumer %s group %s', topic, consumer, group)
                if message2.partition in self.consumers_to_partitions[topic, consumer, group]:
                    await self.consumer_queues[consumer, topic].put(message2)

    def consumer_hello(self, consumer: str) -> str:
        self.consumers_state[consumer] = (Alive, time.time())
        return consumer

    async def consumer_subscribe(self, consumer: str, topic: str, group_id: str) -> Tuple[str, str, str]:
        self.consumers_state[consumer] = (Alive, time.time())
        if group_id not in self.topics_to_groups[topic]:
            self.topics_to_groups[topic].append(group_id)
        self.consumer_queues[(consumer, topic)] = asyncio.Queue()
        self.consumers_to_groups[consumer] = group_id
        self.topics_to_consumers[(topic, group_id)].append(consumer)
        self.consumers_to_topics[consumer].append(topic)
        self.consumer_rebalance(topic, group_id)
        await self.preload_queue(consumer, topic)
        return (consumer, topic, group_id)

    def consumer_unsubscribe_all(self, consumer: str) -> str:
        logger.debug('consumer_unsubscribe_all start')
        logger.debug('consumer: %s', consumer)
        group_id = self.consumers_to_groups[consumer]
        logger.debug('group_id: %s', group_id)
        del self.consumers_to_groups[consumer]
        for topic in self.consumers_to_topics[consumer]:
            logger.debug('topic %s', topic)
            logger.debug('topics_to_consumers %s', self.topics_to_consumers)
            logger.debug('topics_to_consumers %s', self.topics_to_consumers[(topic, group_id)])
            del self.consumer_queues[(consumer, topic)]
            if consumer in self.topics_to_consumers[(topic, group_id)]:
                self.topics_to_consumers[(topic, group_id)].remove(consumer)
            if (topic, consumer, group_id) in self.consumers_to_partitions:
                del self.consumers_to_partitions[(topic, consumer, group_id)]
            self.consumer_rebalance(topic, group_id)
        self.consumers_to_topics[consumer] = list()
        logger.debug('consumer_unsubscribe_all done')
        return consumer

    def consumer_rebalance(self, topic: str, group_id: str) -> None:
        logger.debug('consumer_rebalance start')
        for key in list(self.consumers_to_partitions.keys()):
            if key[0] == topic and key[2] == group_id:
                del self.consumers_to_partitions[key]
        logger.debug('topics_to_consumers: %s', self.topics_to_consumers[(topic, group_id)])
        if len(self.topics_to_consumers[(topic, group_id)]) > 0:
            consumers = cycle(self.topics_to_consumers[(topic, group_id)])
            partitions = self.partitions[topic]
            logger.debug('partitions: %s', partitions)
            for partition in partitions:
                self.consumers_to_partitions[(topic, next(consumers), group_id)].append(partition)
        logger.debug('consumer_rebalance done')

    async def get(self, consumer: str, topic: str) -> FakeKafkaOffsetMessage:
        group_id = self.consumers_to_groups[consumer]
        for partition in self.consumers_to_partitions[(topic, consumer, group_id)]:
            value = await self.consumer_queues[(consumer, topic)].get()
            self.partition_offsets[(topic, partition, group_id)] += 1
            return value
        else:
            raise NoAvailablePartition('No available partition')

    async def preload_queue(self, consumer: str, topic: str) -> None:
        group_id = self.consumers_to_groups[consumer]
        for partition in self.consumers_to_partitions[(topic, consumer, group_id)]:
            logger.debug('preload_queue %s %s %s', consumer, topic, partition)
            offset = self.partition_offsets[(topic, partition, group_id)]
            for message in self.topics[topic][partition][offset:]:
                logger.debug('preload %s', message)
                await self.consumer_queues[(consumer, topic)].put(message)

    async def seek(self, consumer: str, topic: str, partition: int, offset: int) -> None:
        logger.debug('seek %s %s %s %s', consumer, topic, partition, offset)
        group_id = self.consumers_to_groups[consumer]
        self.partition_offsets[(topic, partition, group_id)] = offset
        await self.preload_queue(consumer, topic)
