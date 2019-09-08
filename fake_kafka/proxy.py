
from .messages import FakeKafkaMessage
from urllib.parse import urljoin
from typing import Union
import logging
import aiohttp
from typing import TYPE_CHECKING, Any, List
if TYPE_CHECKING:
    from .consumer import AIOKafkaConsumer
else:
    AIOKafkaConsumer = None


logger = logging.getLogger(__name__)


class _NullResponse:

    async def receive_json(self) -> Any:
        pass

    async def send_json(self, data: Any) -> None:
        pass

    async def close(self) -> None:
        pass


NullResponse = _NullResponse()


class FakeKafkaServerProxy:

    def __init__(self, address: str, use_websocket: bool=False) -> None:
        self.address = address
        self.session = aiohttp.ClientSession()
        self.use_websocket = use_websocket
        self.consumer_websocket: Union[aiohttp.ClientWebSocketResponse, _NullResponse] = NullResponse
        self.producer_websocket: Union[aiohttp.ClientWebSocketResponse, _NullResponse] = NullResponse

    async def consumer_subscribe(self, consumer: str, topic: str, group_id: str) -> None:
        await self.session.post(urljoin(self.address, '/subscription/'),
                                json=dict(consumer_id=consumer,
                                          topic=topic,
                                          group_id=group_id))

    async def get(self, consumer: str, topic: str) -> List[Any]:
        if self.use_websocket and self.consumer_websocket == NullResponse:
            self.consumer_websocket = await self.session.ws_connect(urljoin(self.address,
                                                                            '/consumer_topic_message_ws/{}/{}'.format(topic, consumer)))

            logger.debug('connected consumer_websocket')
        if self.use_websocket:
            try:
                data = await self.consumer_websocket.receive_json()
                logger.debug('data %s', data)
                return data
            except TypeError as e:
                logger.error(e)
                return []
        else:
            response = await self.session.get(urljoin(self.address,
                                                      '/topic_message/{topic}/{consumer_id}'.format(topic=topic,
                                                                                                    consumer_id=consumer)))
            return await response.json()

    async def send(self, topic: str, message: FakeKafkaMessage) -> Any:
        if self.use_websocket and self.producer_websocket == NullResponse:
            self.producer_websocket = await self.session.ws_connect(urljoin(self.address,
                                                                            '/producer_topic_message_ws'))

            logger.debug('connected producer_websocket')
        if self.use_websocket:
            return await self.producer_websocket.send_json(dict(topic=topic,
                                                                partition=message.partition,
                                                                key=message.key,
                                                                value=message.value,
                                                                timestamp=message.timestamp))
        else:
            return await self.session.post(urljoin(self.address,
                                                   '/topic_message/'),
                                           json=dict(topic=topic,
                                                     partition=message.partition,
                                                     key=message.key,
                                                     value=message.value,
                                                     timestamp=message.timestamp))

    async def all_partitions(self, topic: str) -> List[int]:
        response = await self.session.get(urljoin(self.address,
                                                  '/all_partitions/{topic}'.format(topic=topic)))
        return await response.json()

    async def seek(self, consumer: str, topic: str, partition: int, offset: int) -> None:

        await self.session.post(urljoin(self.address,
                                        "/topic_partition_offset/"),
                                json=dict(consumer_id=consumer,
                                          topic=topic,
                                          partition=partition,
                                          offset=offset))
        return

    async def close(self) -> None:
        if self.producer_websocket is not None:
            await self.producer_websocket.close()
        if self.consumer_websocket is not None:
            await self.consumer_websocket.close()
        if self.session is not None:
            await self.session.close()
