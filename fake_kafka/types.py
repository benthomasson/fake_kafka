
from typing import NamedTuple


class TopicPartition(NamedTuple):
    topic: str
    partition: int
