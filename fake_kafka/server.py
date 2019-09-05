
import time
import logging

logger = logging.getLogger(__name__)

from collections import defaultdict

from itertools import cycle



class _Unknown:
    pass


Unknown = _Unknown


class _Alive:
    pass


Alive = _Alive


class _Dead:
    pass


Dead = _Dead


# Singleton FakeKafkaServer
class FakeKafkaServer:

    __instance = None

    def __new__(cls):
        if FakeKafkaServer.__instance is None:
            FakeKafkaServer.__instance = object.__new__(cls)
            FakeKafkaServer._init_topics()
        return FakeKafkaServer.__instance

    @classmethod
    def _init_topics(cls, partitions_per_topic=1):
        cls.__instance.topics = defaultdict(cls.__instance.build_partitions)
        cls.__instance.partitions = defaultdict(lambda: list(range(partitions_per_topic)))
        cls.__instance.consumers_state = defaultdict(lambda: (Unknown, None))
        cls.__instance.topics_to_consumers = defaultdict(list)
        cls.__instance.consumers_to_topics = defaultdict(list)
        cls.__instance.consumers_to_groups = defaultdict(lambda: None)
        cls.__instance.consumers_to_partitions = defaultdict(lambda: -1)
        cls.__instance.partition_offsets = defaultdict(lambda: 0)

    def build_partitions(self):
        return defaultdict(list)

    async def all_partitions(self, topic):
        return self.partitions[topic]

    async def send(self, topic, message):
        message = message._replace(offset=len(self.topics[topic][message.partition]))
        self.topics[topic][message.partition].append(message)

    def consumer_hello(self, consumer):
        self.consumers_state[consumer] = (Alive, time.time())
        return consumer

    async def consumer_subscribe(self, consumer, topic, group_id):
        self.consumers_to_groups[consumer] = group_id
        self.consumers_state[consumer] = (Alive, time.time())
        self.topics_to_consumers[(topic, group_id)].append(consumer)
        self.consumers_to_topics[consumer].append(topic)
        self.consumer_rebalance(topic, group_id)
        return (consumer, topic, group_id)

    def consumer_unsubscribe_all(self, consumer):
        logger.debug('consumer_unsubscribe_all start')
        logger.debug('consumer: %s', consumer)
        group_id = self.consumers_to_groups[consumer]
        logger.debug('group_id: %s', group_id)
        del self.consumers_to_groups[consumer]
        for topic in self.consumers_to_topics[consumer]:
            logger.debug('topic %s', topic)
            logger.debug('topics_to_consumers %s', self.topics_to_consumers)
            logger.debug('topics_to_consumers %s', self.topics_to_consumers[(topic, group_id)])
            if consumer in self.topics_to_consumers[(topic, group_id)]:
                self.topics_to_consumers[(topic, group_id)].remove(consumer)
            if (topic, consumer, group_id) in self.consumers_to_partitions:
                del self.consumers_to_partitions[(topic, consumer, group_id)]
            self.consumer_rebalance(topic, group_id)
        self.consumers_to_topics[consumer] = list()
        logger.debug('consumer_unsubscribe_all done')
        return consumer

    def consumer_rebalance(self, topic, group_id):
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
                self.consumers_to_partitions[(topic, next(consumers), group_id)] = partition
        logger.debug('consumer_rebalance done')

    async def get(self, consumer, topic):
        logger.debug('get consumer: %s topic: %s', consumer, topic)
        group_id = self.consumers_to_groups[consumer]
        logger.debug('topic: %s consumer: %s group_id: %s', topic, consumer, group_id)
        partition = self.consumers_to_partitions[(topic, consumer, group_id)]
        if partition == -1:
            logger.debug('no partition')
            return None
        offset = self.partition_offsets[(topic, partition, group_id)]
        self.partition_offsets[(topic, partition, group_id)] += 1
        if offset < len(self.topics[topic][partition]):
            return self.topics[topic][partition][offset]
        else:
            return None
