
from collections import defaultdict
import random
import time

from ..server import FakeKafkaServer

from ..exceptions import FakeKafkaProducerStateError, FakeKafkaConsumerStateError

from ..messages import FakeKafkaMessage


class AIOKafkaProducer:

    def __init__(self, loop=None, bootstrap_servers=None):
        if bootstrap_servers is None:
            self.server = FakeKafkaServer()
        self.started = False
        self.stopped = False
        self.all_partitions = self.server.all_partitions()
        self.partitions_by_key = defaultdict(self.get_random_partition)

    async def start(self):
        self.started = True
        self.stopped = False

    def get_random_partition(self):
        return random.choice(self.all_partitions)

    async def send_and_wait(self, topic, value, key=None, partition=None, timestamp_ms=None):
        if not self.started:
            raise FakeKafkaProducerStateError('Send occurred when producer had not been started')
        if self.stopped:
            raise FakeKafkaProducerStateError('Send occurred when producer has been stopped')
        offset = None
        if key is None and partition is None:
            partition = self.get_random_partition()
        elif partition is None:
            partition = self.partitions_by_key[key]
        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)
        self.server.send(topic, FakeKafkaMessage(topic, partition, offset, key, value, timestamp_ms))

    async def stop(self):
        if not self.started:
            raise FakeKafkaProducerStateError('Stop occurred when producer had not been started')
        self.stopped = True
