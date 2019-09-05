from fastapi import FastAPI
from starlette.websockets import WebSocket
from pydantic import BaseModel

from .server import FakeKafkaServer
from . import messages

app = FastAPI()


@app.get("/all_partitions/{topic}")
async def all_partitions(topic: str):
    return FakeKafkaServer().all_partitions(topic)


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
    return FakeKafkaServer().get(consumer_id, topic)


class Message(BaseModel):

    topic: str
    partition: int = None
    key: str = None
    value: str
    timestamp: int = None


@app.post("/topic_message/")
async def send_topic_message(message: Message):
    return FakeKafkaServer().send(message.topic, messages.FakeKafkaMessage(message.topic,
                                                                           message.partition,
                                                                           None,
                                                                           message.key,
                                                                           message.value,
                                                                           message.timestamp))


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        data = await websocket.receive_text()
        await websocket.send_text(f"Message text was: {data}")
