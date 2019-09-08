
from typing import NamedTuple, Optional


class FakeKafkaMessage(NamedTuple):
    topic: str
    partition: int
    key: Optional[str]
    value: str
    timestamp: Optional[int]


class FakeKafkaOffsetMessage(NamedTuple):
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: str
    timestamp: Optional[int]
