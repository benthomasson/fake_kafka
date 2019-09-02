

from .server import FakeKafkaServer

from .exceptions import FakeKafkaProducerStateError, FakeKafkaConsumerStateError


class AIOKafkaProducer:

    def __init__(self, loop=None, bootstrap_servers=None):
        if bootstrap_servers is None:
            self.server = FakeKafkaServer()
        self.started = False
        self.stopped = False

    async def start(self):
        self.started = True
        self.stopped = False

    async def send_and_wait(self, topic, message):
        if not self.started:
            raise FakeKafkaProducerStateError('Send occurred when producer had not been started')
        if self.stopped:
            raise FakeKafkaProducerStateError('Send occurred when producer has been stopped')
        self.server.send(topic, message)

    async def stop(self):
        if not self.started:
            raise FakeKafkaProducerStateError('Stop occurred when producer had not been started')
        self.stopped = True


class TopicPartition:

    def __init__(self, topic, partition):
        self.topic = topic
        self.partition = partition


class AIOKafkaConsumer:

    def __init__(self, topic, loop=None, bootstrap_servers=None, group_id=None, auto_offset_reset=None):
        if bootstrap_servers is None:
            self.server = FakeKafkaServer()
        self.topic = topic
        self.started = False
        self.stopped = False
        self.offset = 0

    def check_started(self):
        if not self.started:
            raise FakeKafkaConsumerStateError('Stop occurred when consumer had not been started')

    async def start(self):
        self.started = True
        self.stopped = False

    async def stop(self):
        self.check_started()
        self.stopped = True

    def __aiter__(self):
        self.check_started()
        return self

    async def __anext__(self):
        self.check_started()
        message = self.server.get(self.topic, self.offset)
        if message is None:
            raise StopAsyncIteration
        else:
            return message

    async def getone(self):
        return self.__anext__()

    async def getmany(self, timeout_ms=1000):
        return self.__anext__()

    async def position(self, tp):
        return 0

    async def committed(self, tp):
        return 0

    async def commit(self):
        pass

    async def seek(self, tp, offset):
        pass

    async def assign(self, tp_list):
        pass

    async def assignment(self):
        pass

    async def subscribe(self, topic):
        pass

    async def last_stable_offset(self, tp):
        pass

    async def end_offsets(self, tp_list):
        pass

    async def seek_to_end(self, tp):
        pass
