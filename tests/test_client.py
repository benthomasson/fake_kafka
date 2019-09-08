
import pytest

from fake_kafka.server import Alive
from fake_kafka.consumer import AIOKafkaConsumer
from fake_kafka.producer import AIOKafkaProducer

from test_fake_kafka import fake_kafka_server
from test_proxy import api_server_factory

fake_kafka_server = fake_kafka_server
api_server_factory = api_server_factory


@pytest.mark.asyncio
async def test_subscribe(api_server_factory, fake_kafka_server):
    api_server, _ = api_server_factory()
    consumer = AIOKafkaConsumer(topic="events", bootstrap_servers=["http://127.0.0.1:8000"], group_id='a')
    await consumer.start()
    print(fake_kafka_server.consumers_state)
    assert fake_kafka_server.consumers_state[consumer.consumer_id][0] == Alive
    assert fake_kafka_server.topics_to_consumers[('events', 'a')] == [consumer.consumer_id]
    assert fake_kafka_server.consumers_to_partitions[('events', consumer.consumer_id, 'a')] == [0]
    api_server.join()


@pytest.mark.asyncio
async def test_send(api_server_factory, fake_kafka_server):
    api_server, _ = api_server_factory()
    producer = AIOKafkaProducer(bootstrap_servers=["http://127.0.0.1:8000"])
    await producer.start()
    await producer.send_and_wait("events", "Super message")
    assert fake_kafka_server.topics["events"][0][0].value == "Super message"
    api_server.join()


@pytest.mark.asyncio
async def test_send_and_get(api_server_factory, fake_kafka_server):
    api_server, _ = api_server_factory(3)
    producer = AIOKafkaProducer(bootstrap_servers=["http://127.0.0.1:8000"])
    consumer = AIOKafkaConsumer(topic="events", bootstrap_servers=["http://127.0.0.1:8000"], group_id='a')
    await consumer.start()
    assert fake_kafka_server.consumers_state[consumer.consumer_id][0] == Alive
    assert fake_kafka_server.topics_to_consumers[('events', 'a')] == [consumer.consumer_id]
    assert fake_kafka_server.consumers_to_partitions[('events', consumer.consumer_id, 'a')] == [0]
    await producer.start()
    await producer.send_and_wait("events", "Super message")
    assert fake_kafka_server.topics["events"][0][0].value == "Super message"
    message = await consumer.getone()
    assert message[:-1] == ['events', 0, 0, None, 'Super message']
    api_server.join()
