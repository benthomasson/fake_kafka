

from collections import defaultdict

from itertools import cycle
import time


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
        cls.__instance.topics_to_consumers = defaultdict(set)
        cls.__instance.consumers_to_topics = defaultdict(list)
        cls.__instance.consumers_to_partitions = defaultdict(lambda: -1)
        cls.__instance.partition_offsets = defaultdict(lambda: 0)

    def build_partitions(self):
        return defaultdict(list)

    def all_partitions(self, topic):
        return self.partitions[topic]

    def send(self, topic, message):
        message = message._replace(offset=len(self.topics[topic][message.partition]))
        self.topics[topic][message.partition].append(message)

    def consumer_hello(self, consumer):
        self.consumers_state[consumer] = (Alive, time.time())

    def consumer_subscribe(self, consumer, topic):
        self.consumers_state[consumer] = (Alive, time.time())
        self.topics_to_consumers[topic].add(consumer)
        self.consumers_to_topics[consumer].append(topic)
        self.consumer_rebalance(topic)

    def consumer_unsubscribe_all(self, consumer):
        for topic in self.consumers_to_topics:
            self.topics_to_consumers[topic].remove(consumer)
            del self.consumers_to_partitions[(topic, consumer)]
            self.consumer_rebalance(topic)
        self.consumers_to_topics[consumer] = list()

    def consumer_rebalance(self, topic):
        consumers = cycle(self.topics_to_consumers[topic])
        partitions = self.all_partitions(topic)
        for partition in partitions:
            self.consumers_to_partitions[(topic, next(consumers))] = partition

    def get(self, consumer, topic):
        partition = self.consumers_to_partitions[(topic, consumer)]
        offset = self.partition_offsets[(topic, partition)]
        self.partition_offsets[(topic, partition)] += 1
        if offset < len(self.topics[topic][partition]):
            return self.topics[topic][partition][offset]
        else:
            return None
