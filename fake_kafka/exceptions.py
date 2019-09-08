

class FakeKafkaProducerStateError(Exception):
    pass


class FakeKafkaConsumerStateError(Exception):
    pass


class NoAvailablePartition(Exception):
    pass
