
import uuid

from typing import List, Optional, Union

from .server import FakeKafkaServer
from .types import TopicPartition
from .messages import FakeKafkaOffsetMessage

from .proxy import FakeKafkaServerProxy

from .consumer_fsm import State, NotStarted


class AIOKafkaConsumer:

    def __init__(self, topic: str,
                 bootstrap_servers: Optional[List[str]] = None,
                 group_id: str = '',
                 use_websocket: bool = False) -> None:
        self.server: Union[FakeKafkaServer, FakeKafkaServerProxy] = FakeKafkaServer() if bootstrap_servers is None \
            else FakeKafkaServerProxy(bootstrap_servers[0], use_websocket=use_websocket)
        self.consumer_id = str(uuid.uuid4())
        self.group_id = group_id
        self.topic = topic
        self.started = False
        self.stopped = False
        self.offset = 0
        self.state: State = NotStarted

    async def change_state(self, state: State) -> None:
        await self.state.exit(self)
        self.state = state
        await self.state.enter(self)

    async def start(self) -> None:
        await self.state.start(self)

    async def stop(self) -> None:
        await self.state.stop(self)

    def __aiter__(self) -> 'AIOKafkaConsumer':
        return self.state.__aiter__(self)

    async def __anext__(self) -> FakeKafkaOffsetMessage:
        return await self.state.__anext__(self)

    async def getone(self) -> FakeKafkaOffsetMessage:
        return await self.state.__anext__(self)

    async def seek(self, tp: TopicPartition, offset: int) -> None:
        await self.state.seek(self, tp, offset)
