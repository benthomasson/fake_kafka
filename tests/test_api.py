
import asyncio
import pytest

from starlette.testclient import TestClient

from fake_kafka.api import app
from fake_kafka.server import FakeKafkaServer, Alive, Unknown

from test_fake_kafka import fake_kafka_server


@pytest.fixture
def test_client():
    return TestClient(app)


def test_all_partitions(test_client):
    response = test_client.get("/all_partitions/topic")
    assert response.status_code == 200
    assert response.json() == [0]


def test_consumer_hello(test_client, fake_kafka_server):
    assert fake_kafka_server.consumers_state['1'][0] == Unknown
    response = test_client.post("/consumer/", json=dict(consumer_id="1"))
    assert response.status_code == 200
    assert response.json() == "1"
    assert fake_kafka_server.consumers_state['1'][0] == Alive


def test_consumer_subscribe(test_client, fake_kafka_server):
    assert fake_kafka_server.consumers_state['1'][0] == Unknown
    response = test_client.post("/subscription/", json=dict(consumer_id="1", topic="events", group_id="a"))
    assert response.status_code == 200
    assert response.json() == ['1', 'events', 'a']
    assert fake_kafka_server.consumers_state['1'][0] == Alive
    assert fake_kafka_server.topics_to_consumers[('events', 'a')] == ['1']
    assert fake_kafka_server.consumers_to_partitions[('events', '1', 'a')] == 0


def test_consumer_unsubscribe(test_client, fake_kafka_server):
    test_consumer_subscribe(test_client, fake_kafka_server)
    response = test_client.delete("/subscription/1")
    assert response.status_code == 200
    assert response.json() == '1'
    assert fake_kafka_server.consumers_state['1'][0] == Alive


def test_send_message(test_client, fake_kafka_server):
    response = test_client.post("/topic_message/", json=dict(topic="events",
                                                             partition=0,
                                                             value="hello"))
    assert response.status_code == 200
    assert response.json() == None
    assert fake_kafka_server.topics["events"][0][0].value == "hello"


def test_get_message(test_client, fake_kafka_server):
    test_send_message(test_client, fake_kafka_server)
    test_consumer_subscribe(test_client, fake_kafka_server)
    response = test_client.get('/topic_message/events/1')
    assert response.status_code == 200
    assert response.json() == ['events', 0, 0, None, 'hello', None]
