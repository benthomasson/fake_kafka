import logging
from typing import Union


from fastapi import FastAPI
from starlette.websockets import WebSocket
from pydantic import BaseModel

from .server import FakeKafkaServer
from . import messages

from starlette.websockets import WebSocketDisconnect

IntNone = Union[int, None]
StrNone = Union[str, None]

logger = logging.getLogger(__name__)

app = FastAPI()


@app.get("/all_partitions/{topic}")
async def all_partitions(topic: str):
    return await FakeKafkaServer().all_partitions(topic)


class Consumer(BaseModel):

    consumer_id: str


@app.post("/consumer/")
async def consumer_hello(consumer: Consumer):
    return FakeKafkaServer().consumer_hello(consumer.consumer_id)


class Subscription(BaseModel):

    consumer_id: str
    topic: str
    group_id: str


@app.post("/subscription/")
async def create_subscription(subscription: Subscription):
    return await FakeKafkaServer().consumer_subscribe(subscription.consumer_id,
                                                      subscription.topic,
                                                      subscription.group_id)


@app.delete("/subscription/{consumer_id}")
async def consumer_unsubscribe(consumer_id: str):
    return FakeKafkaServer().consumer_unsubscribe_all(consumer_id)


@app.get("/topic_message/{topic}/{consumer_id}")
async def get_topic_message(topic: str, consumer_id: str):
    return await FakeKafkaServer().get(consumer_id, topic)


class TopicPartitionOffset(BaseModel):

    consumer_id: str
    topic: str
    partition: int
    offset: int


@app.post("/topic_partition_offset/")
async def topic_partition_offset(tpo: TopicPartitionOffset):
    await FakeKafkaServer().seek(tpo.consumer_id, tpo.topic, tpo.partition, tpo.offset)
    return


class Message(BaseModel):

    topic: str
    partition: IntNone = None
    key: StrNone = None
    value: str
    timestamp: IntNone = None


@app.post("/topic_message/")
async def send_topic_message(message: Message):
    return await FakeKafkaServer().send(message.topic, messages.FakeKafkaMessage(message.topic,
                                                                           message.partition,
                                                                           None,
                                                                           message.key,
                                                                           message.value,
                                                                           message.timestamp))


@app.websocket("/producer_topic_message_ws")
async def producer_topic_message_ws(websocket: WebSocket):
    await websocket.accept()
    while True:
        try:
            message = await websocket.receive_json()
            logger.debug(message)
            await FakeKafkaServer().send(message['topic'], messages.FakeKafkaMessage(message['topic'],
                                                                               message['partition'],
                                                                               None,
                                                                               message.get('key'),
                                                                               message['value'],
                                                                               message.get('timestamp')))
        except WebSocketDisconnect:
            break


@app.websocket("/consumer_topic_message_ws/{topic}/{consumer_id}")
async def consumer_topic_message_ws(websocket: WebSocket, topic: str, consumer_id: str):
    await websocket.accept()
    server = FakeKafkaServer()
    while True:
        try:
            message = await server.get(consumer_id, topic)
            if message is None:
                break
            await websocket.send_json(message)
        except WebSocketDisconnect:
            break
