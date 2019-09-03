

from ..server import FakeKafkaServer
from ..exceptions import FakeKafkaConsumerStateError


class State:

    def enter(self, machine):
        pass

    def exit(self, machine):
        pass


class _NotStarted(State):

    def start(self, machine):
        machine.change_state(Started)

    def stop(self, machine):
        raise FakeKafkaConsumerStateError('Stop occurred when consumer had not been started')

    def __aiter__(self, machine):
        raise FakeKafkaConsumerStateError('Consumer had not been started')

    def __anext__(self, machine):
        raise FakeKafkaConsumerStateError('Consumer had not been started')


NotStarted = _NotStarted()


class _Started(State):

    def enter(self, machine):
        machine.started = True
        machine.server.consumer_subscribe(machine, machine.topic)

    def exit(self, machine):
        machine.started = False

    def start(self, machine):
        pass

    def stop(self, machine):
        machine.change_state(Stopped)

    def __aiter__(self, machine):
        return machine

    async def __anext__(self, machine):
        message = machine.server.get(machine, machine.topic)
        machine.offset += 1
        if message is None:
            raise StopAsyncIteration
        else:
            return message


Started = _Started()


class _Stopped(State):

    def enter(self, machine):
        machine.stopped = True

    def exit(self, machine):
        raise FakeKafkaConsumerStateError('Consumers cannot be restarted')

    def __aiter__(self, machine):
        raise FakeKafkaConsumerStateError('Consumer has been stopped')

    def __anext__(self, machine):
        raise FakeKafkaConsumerStateError('Consumer has been stopped')


Stopped = _Stopped()


class AIOKafkaConsumer:

    def __init__(self, topic, loop=None, bootstrap_servers=None, group_id=None, auto_offset_reset=None):
        if bootstrap_servers is None:
            self.server = FakeKafkaServer()
        self.loop = loop
        self.topic = topic
        self.started = False
        self.stopped = False
        self.offset = 0
        self.state = NotStarted

    def change_state(self, state):
        self.state.exit(self)
        self.state = state
        self.state.enter(self)

    async def start(self):
        self.state.start(self)

    async def stop(self):
        self.state.stop(self)

    def __aiter__(self):
        return self.state.__aiter__(self)

    async def __anext__(self):
        return await self.state.__anext__(self)

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
