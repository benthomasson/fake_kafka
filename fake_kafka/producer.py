
from typing import Optional, List, Union, Dict, Iterator, cast
import time
from itertools import cycle

from .server import FakeKafkaServer

from .exceptions import FakeKafkaProducerStateError

from .messages import FakeKafkaMessage
from .proxy import FakeKafkaServerProxy


class AIOKafkaProducer:

    def __init__(self, bootstrap_servers: Optional[List[str]] = None, use_websocket: bool = False):
        self.server: Union[FakeKafkaServer, FakeKafkaServerProxy] = FakeKafkaServer() if bootstrap_servers is None \
            else FakeKafkaServerProxy(bootstrap_servers[0], use_websocket=use_websocket)
        self.started = False
        self.stopped = False
        self.all_partitions_cycle: Dict[str, Iterator[int]] = dict()
        self.partitions_by_key: Dict[str, int] = dict()

    async def start(self) -> None:
        self.started = True
        self.stopped = False

    async def get_next_partition(self, topic: str) -> int:
        if topic not in self.all_partitions_cycle:
            self.all_partitions_cycle[topic] = cycle(await self.server.all_partitions(topic))
        return next(self.all_partitions_cycle[topic])

    async def send_and_wait(self,
                            topic: str,
                            value: str,
                            key: Optional[str] = None,
                            partition: Optional[int] = None,
                            timestamp_ms: Optional[int] = None) -> None:
        if not self.started:
            raise FakeKafkaProducerStateError('Send occurred when producer had not been started')
        if self.stopped:
            raise FakeKafkaProducerStateError('Send occurred when producer has been stopped')
        if key is None and partition is None:
            partition = await self.get_next_partition(topic)
        elif partition is None:
            key2 = cast(str, key)
            if key2 not in self.partitions_by_key:
                self.partitions_by_key[key2] = await self.get_next_partition(topic)
            partition = self.partitions_by_key[key2]
        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)
        await self.server.send(topic, FakeKafkaMessage(topic, partition, key, value, timestamp_ms))

    async def stop(self) -> None:
        if not self.started:
            raise FakeKafkaProducerStateError('Stop occurred when producer had not been started')
        self.stopped = True
