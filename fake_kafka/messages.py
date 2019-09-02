
from collections import namedtuple


FakeKafkaMessage = namedtuple('FakeKafkaMessage', ['topic', 'partition', 'offset', 'key', 'value', 'timestamp'])
