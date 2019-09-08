
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .consumer import AIOKafkaConsumer
else:
    AIOKafkaConsumer = None


from typing import Union

from .exceptions import FakeKafkaConsumerStateError
from .types import TopicPartition
from .messages import FakeKafkaOffsetMessage


class _NotStarted:

    async def enter(self, machine: AIOKafkaConsumer) -> None:
        pass

    async def exit(self, machine: AIOKafkaConsumer) -> None:
        pass

    async def start(self, machine: AIOKafkaConsumer) -> None:
        await machine.change_state(Started)

    async def stop(self, machine: AIOKafkaConsumer) -> None:
        raise FakeKafkaConsumerStateError('Stop occurred when consumer had not been started')

    def __aiter__(self, machine: AIOKafkaConsumer) -> AIOKafkaConsumer:
        raise FakeKafkaConsumerStateError('Consumer had not been started')

    async def __anext__(self, machine: AIOKafkaConsumer) -> FakeKafkaOffsetMessage:
        raise FakeKafkaConsumerStateError('Consumer had not been started')

    async def seek(self, machine: AIOKafkaConsumer, tp: TopicPartition, offset: int) -> None:
        raise FakeKafkaConsumerStateError('Consumer had not been started')


NotStarted = _NotStarted()


class _Started:

    async def enter(self, machine: AIOKafkaConsumer) -> None:
        machine.started = True
        await machine.server.consumer_subscribe(machine.consumer_id, machine.topic, machine.group_id)

    async def exit(self, machine: AIOKafkaConsumer) -> None:
        machine.started = False

    async def start(self, machine: AIOKafkaConsumer) -> None:
        pass

    async def stop(self, machine: AIOKafkaConsumer) -> None:
        await machine.change_state(Stopped)

    def __aiter__(self, machine: AIOKafkaConsumer) -> AIOKafkaConsumer:
        return machine

    async def __anext__(self, machine: AIOKafkaConsumer) -> FakeKafkaOffsetMessage:
        message = await machine.server.get(machine.consumer_id, machine.topic)
        machine.offset += 1
        if message is None:
            raise StopAsyncIteration
        else:
            return message

    async def seek(self, machine: AIOKafkaConsumer, tp: TopicPartition, offset: int) -> None:
        await machine.server.seek(machine.consumer_id, tp.topic, tp.partition, offset)


Started = _Started()


class _Stopped:

    async def enter(self, machine: AIOKafkaConsumer) -> None:
        machine.stopped = True

    async def exit(self, machine: AIOKafkaConsumer) -> None:
        raise FakeKafkaConsumerStateError('Consumers cannot be restarted')

    def __aiter__(self, machine: AIOKafkaConsumer) -> AIOKafkaConsumer:
        raise FakeKafkaConsumerStateError('Consumer has been stopped')

    async def __anext__(self, machine: AIOKafkaConsumer) -> FakeKafkaOffsetMessage:
        raise FakeKafkaConsumerStateError('Consumer has been stopped')

    async def start(self, machine: AIOKafkaConsumer) -> None:
        pass

    async def stop(self, machine: AIOKafkaConsumer) -> None:
        pass

    async def seek(self, machine: AIOKafkaConsumer, tp: TopicPartition, offset: int) -> None:
        raise FakeKafkaConsumerStateError('Consumer has been stopped')


Stopped = _Stopped()


State = Union[_NotStarted, _Started, _Stopped]
