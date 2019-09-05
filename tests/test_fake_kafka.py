"""Tests for `fake_kafka` package."""

import asyncio
import pytest
import fake_kafka
from fake_kafka.server import Alive


@pytest.fixture
def fake_kafka_server():
    server = fake_kafka.FakeKafkaServer()
    fake_kafka.FakeKafkaServer._init_topics(partitions_per_topic=1)
    return server


@pytest.fixture
def loop():
    return asyncio.get_event_loop()


def test_server(fake_kafka_server):
    assert len(fake_kafka_server.topics) == 0


@pytest.mark.asyncio
async def test_client_async_for():

    consumer = fake_kafka.AIOKafkaConsumer('test')
    await consumer.start()
    assert consumer.started
    ai = consumer.__aiter__()
    with pytest.raises(StopAsyncIteration):
        await ai.__anext__()
    await consumer.stop()
    assert consumer.stopped


@pytest.mark.asyncio
async def test_producer(loop, fake_kafka_server):
    producer = fake_kafka.AIOKafkaProducer(loop=loop)
    await producer.start()
    assert producer.started
    try:
        await producer.send_and_wait("my_topic", b"Super message")
    finally:
        await producer.stop()
    assert producer.stopped
    assert len(fake_kafka_server.topics['my_topic']) == 1
    assert fake_kafka_server.topics['my_topic'][0][0].value == b"Super message"


@pytest.mark.asyncio
async def test_consumer(loop, fake_kafka_server):
    consumer = fake_kafka.AIOKafkaConsumer("my_topic", loop=loop)
    await consumer.start()
    assert fake_kafka_server.consumers_state[consumer][0] == Alive
    assert consumer in fake_kafka_server.topics_to_consumers[("my_topic", None)]
    assert fake_kafka_server.consumers_to_topics[consumer] == ['my_topic']
    assert fake_kafka_server.consumers_to_partitions[('my_topic', consumer, None)] == 0
    assert consumer.started
    ai = consumer.__aiter__()
    with pytest.raises(StopAsyncIteration):
        await ai.__anext__()
    await consumer.stop()
    assert consumer.stopped


@pytest.mark.asyncio
async def test_producer_and_consumer(loop, fake_kafka_server):
    producer = fake_kafka.AIOKafkaProducer(loop=loop)
    await producer.start()
    assert producer.started
    consumer = fake_kafka.AIOKafkaConsumer("my_topic", loop=loop)
    await consumer.start()
    assert consumer.started
    try:
        await producer.send_and_wait("my_topic", b"Super message1")
        await producer.send_and_wait("my_topic", b"Super message2")
        assert len(fake_kafka_server.topics['my_topic'][0]) == 2
        assert fake_kafka_server.topics['my_topic'][0][0].value == b"Super message1"
        assert fake_kafka_server.topics['my_topic'][0][1].value == b"Super message2"
        ai = consumer.__aiter__()
        msg = await ai.__anext__()
        assert msg.partition == 0
        assert msg.offset == 0
        assert msg.value == b"Super message1"
        msg = await ai.__anext__()
        assert msg.partition == 0
        assert msg.offset == 1
        assert msg.value == b"Super message2"
    finally:
        await consumer.stop()
        await producer.stop()
    assert consumer.stopped
    assert producer.stopped


@pytest.mark.asyncio
async def test_producer_and_consumer_same_group(loop, fake_kafka_server):
    fake_kafka.FakeKafkaServer._init_topics(partitions_per_topic=2)
    producer = fake_kafka.AIOKafkaProducer(loop=loop)
    await producer.start()
    assert producer.started
    consumer1 = fake_kafka.AIOKafkaConsumer("my_topic", loop=loop, group_id='a')
    consumer2 = fake_kafka.AIOKafkaConsumer("my_topic", loop=loop, group_id='a')
    await consumer1.start()
    await consumer2.start()
    assert consumer1.started
    assert consumer2.started
    try:
        await producer.send_and_wait("my_topic", b"Super message1")
        await producer.send_and_wait("my_topic", b"Super message2")
        assert len(fake_kafka_server.topics['my_topic'][0]) == 1
        assert fake_kafka_server.topics['my_topic'][0][0].value == b"Super message1"
        assert fake_kafka_server.topics['my_topic'][1][0].value == b"Super message2"
        ai = consumer1.__aiter__()
        msg = await ai.__anext__()
        assert msg.partition == 0
        assert msg.offset == 0
        assert msg.value == b"Super message1"
        ai = consumer2.__aiter__()
        msg = await ai.__anext__()
        assert msg.partition == 1
        assert msg.offset == 0
        assert msg.value == b"Super message2"
    finally:
        await consumer1.stop()
        await producer.stop()
        await consumer2.stop()
    assert consumer1.stopped
    assert consumer2.stopped
    assert producer.stopped


@pytest.mark.asyncio
async def test_producer_and_consumer_different_groups(loop, fake_kafka_server):
    producer = fake_kafka.AIOKafkaProducer(loop=loop)
    await producer.start()
    assert producer.started
    consumer1 = fake_kafka.AIOKafkaConsumer("my_topic", loop=loop, group_id='a')
    consumer2 = fake_kafka.AIOKafkaConsumer("my_topic", loop=loop, group_id='b')
    await consumer1.start()
    await consumer2.start()
    assert consumer1.started
    assert consumer2.started
    try:
        await producer.send_and_wait("my_topic", b"Super message1")
        await producer.send_and_wait("my_topic", b"Super message2")
        assert len(fake_kafka_server.topics['my_topic'][0]) == 2
        assert fake_kafka_server.topics['my_topic'][0][0].value == b"Super message1"
        assert fake_kafka_server.topics['my_topic'][0][1].value == b"Super message2"
        ai = consumer1.__aiter__()
        msg = await ai.__anext__()
        assert msg.partition == 0
        assert msg.offset == 0
        assert msg.value == b"Super message1"
        msg = await ai.__anext__()
        assert msg.partition == 0
        assert msg.offset == 1
        assert msg.value == b"Super message2"
        ai2 = consumer2.__aiter__()
        msg = await ai2.__anext__()
        assert msg.partition == 0
        assert msg.offset == 0
        assert msg.value == b"Super message1"
        msg = await ai2.__anext__()
        assert msg.partition == 0
        assert msg.offset == 1
        assert msg.value == b"Super message2"
    finally:
        await consumer1.stop()
        await producer.stop()
        await consumer2.stop()
    assert consumer1.stopped
    assert consumer2.stopped
    assert producer.stopped
