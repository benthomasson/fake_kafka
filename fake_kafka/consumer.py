
import uuid

from .server import FakeKafkaServer
from .exceptions import FakeKafkaConsumerStateError

from .proxy import FakeKafkaServerProxy


class State:

    async def enter(self, machine):
        pass

    async def exit(self, machine):
        pass


class _NotStarted(State):

    async def start(self, machine):
        await machine.change_state(Started)

    async def stop(self, machine):
        raise FakeKafkaConsumerStateError('Stop occurred when consumer had not been started')

    def __aiter__(self, machine):
        raise FakeKafkaConsumerStateError('Consumer had not been started')

    def __anext__(self, machine):
        raise FakeKafkaConsumerStateError('Consumer had not been started')


NotStarted = _NotStarted()


class _Started(State):

    async def enter(self, machine):
        machine.started = True
        await machine.server.consumer_subscribe(machine, machine.topic, machine.group_id)

    async def exit(self, machine):
        machine.started = False

    async def start(self, machine):
        pass

    async def stop(self, machine):
        await machine.change_state(Stopped)

    def __aiter__(self, machine):
        return machine

    async def __anext__(self, machine):
        message = await machine.server.get(machine, machine.topic)
        machine.offset += 1
        if message is None:
            raise StopAsyncIteration
        else:
            return message


Started = _Started()


class _Stopped(State):

    async def enter(self, machine):
        machine.stopped = True

    async def exit(self, machine):
        raise FakeKafkaConsumerStateError('Consumers cannot be restarted')

    def __aiter__(self, machine):
        raise FakeKafkaConsumerStateError('Consumer has been stopped')

    def __anext__(self, machine):
        raise FakeKafkaConsumerStateError('Consumer has been stopped')


Stopped = _Stopped()


class AIOKafkaConsumer:

    def __init__(self, topic, loop=None, bootstrap_servers=None, group_id=None, auto_offset_reset=None, use_websocket=False):
        if bootstrap_servers is None:
            self.server = FakeKafkaServer()
        else:
            self.server = FakeKafkaServerProxy(bootstrap_servers[0], use_websocket=use_websocket)
        self.consumer_id = str(uuid.uuid4())
        self.loop = loop
        self.group_id = group_id
        self.topic = topic
        self.started = False
        self.stopped = False
        self.offset = 0
        self.state = NotStarted

    async def change_state(self, state):
        await self.state.exit(self)
        self.state = state
        await self.state.enter(self)

    async def start(self):
        await self.state.start(self)

    async def stop(self):
        await self.state.stop(self)

    def __aiter__(self):
        return self.state.__aiter__(self)

    async def __anext__(self):
        return await self.state.__anext__(self)

    async def getone(self):
        return await self.state.__anext__(self)
