
import asyncio
import pytest
import threading
import time
import requests

from fake_kafka.proxy import FakeKafkaServerProxy
from fake_kafka.server import Alive, Unknown
from fake_kafka.messages import FakeKafkaMessage

import fake_kafka.api
from fake_kafka.api import Consumer

from uvicorn.config import Config
from uvicorn.main import Server

from test_fake_kafka import fake_kafka_server


@pytest.fixture
def api_server_factory():

    def _factory(limit_max_requests=1):

        class CustomServer(Server):
            def install_signal_handlers(self):
                pass

        config = Config(app=fake_kafka.api.app,
                        loop="asyncio",
                        limit_max_requests=limit_max_requests)
        server = CustomServer(config=config)
        thread = threading.Thread(target=server.run)
        thread.start()
        while not server.started:
            time.sleep(0.01)
        return thread, server
    return _factory


@pytest.mark.asyncio
async def test_subscribe(api_server_factory, fake_kafka_server):
    api_server, _ = api_server_factory()
    assert fake_kafka_server.consumers_state['1'][0] == Unknown
    proxy = FakeKafkaServerProxy("http://127.0.0.1:8000")
    await proxy.consumer_subscribe(Consumer(consumer_id='1'), 'events', 'a')
    assert fake_kafka_server.consumers_state['1'][0] == Alive
    assert fake_kafka_server.topics_to_consumers[('events', 'a')] == ['1']
    assert fake_kafka_server.consumers_to_partitions[('events', '1', 'a')] == 0
    api_server.join()


@pytest.mark.asyncio
async def test_send_message(api_server_factory, fake_kafka_server):
    api_server, _ = api_server_factory()
    proxy = FakeKafkaServerProxy("http://127.0.0.1:8000")
    response = await proxy.send('events', FakeKafkaMessage("events",
                                                0,
                                                None,
                                                None,
                                                "hello",
                                                int(time.time() * 1000)))
    assert response.status == 200, await response.text()
    assert fake_kafka_server.topics["events"][0][0].value == "hello"
    api_server.join()


@pytest.mark.asyncio
async def test_send_get_message(api_server_factory, fake_kafka_server):
    api_server, _ = api_server_factory(2)
    proxy = FakeKafkaServerProxy("http://127.0.0.1:8000")
    await proxy.consumer_subscribe(Consumer(consumer_id='1'), 'events', 'a')
    await proxy.send('events', FakeKafkaMessage("events",
                                                0,
                                                None,
                                                None,
                                                "hello",
                                                42))
    assert fake_kafka_server.topics["events"][0][0].value == "hello"
    response = await proxy.get(Consumer(consumer_id='1'), 'events')
    assert response == ['events', 0, 0, None, 'hello', 42]
    api_server.join()


@pytest.mark.asyncio
async def test_send_message_web_socket(api_server_factory, fake_kafka_server):
    api_server, _ = api_server_factory()
    proxy = FakeKafkaServerProxy("http://127.0.0.1:8000", use_websocket=True)
    await proxy.send('events', FakeKafkaMessage("events",
                                                0,
                                                None,
                                                None,
                                                "hello",
                                                int(time.time() * 1000)))
    # A sleep is needed here since the server is running in a different thread
    await asyncio.sleep(0.1)
    assert fake_kafka_server.topics["events"][0][0].value == "hello"
    await proxy.close()
    # Work-around to get the server thread to close.  We need a single non-websocket request.
    requests.get("http://127.0.0.1:8000/docs")
    api_server.join()


@pytest.mark.timeout(10)
@pytest.mark.asyncio
async def test_send_get_message_websocket(api_server_factory, fake_kafka_server):
    api_server_thread, api_server = api_server_factory(3)
    proxy = FakeKafkaServerProxy("http://127.0.0.1:8000", use_websocket=True)
    await proxy.consumer_subscribe(Consumer(consumer_id='1'), 'events', 'a')
    await proxy.send('events', FakeKafkaMessage("events",
                                                0,
                                                None,
                                                None,
                                                "hello",
                                                42))
    await asyncio.sleep(1)
    response = await proxy.get(Consumer(consumer_id='1'), 'events')
    assert response == ['events', 0, 0, None, 'hello', 42]
    await proxy.close()
    # Work-around to get the server thread to close.  We need a single non-websocket request.
    requests.get("http://127.0.0.1:8000/docs")
    api_server.should_exit = True
    api_server.force_exit = True
    api_server_thread.join()


@pytest.mark.asyncio
async def test_seek(api_server_factory, fake_kafka_server):
    api_server, _ = api_server_factory(2)
    proxy = FakeKafkaServerProxy("http://127.0.0.1:8000")
    await proxy.consumer_subscribe(Consumer(consumer_id='1'), 'events', 'a')
    await proxy.send('events', FakeKafkaMessage("events",
                                                0,
                                                None,
                                                None,
                                                "hello",
                                                42))
    assert fake_kafka_server.topics["events"][0][0].value == "hello"
    response = await proxy.get(Consumer(consumer_id='1'), 'events')
    assert response == ['events', 0, 0, None, 'hello', 42]
    await proxy.seek(Consumer(consumer_id='1'), 'events', 0, 0)
    response = await proxy.get(Consumer(consumer_id='1'), 'events')
    assert response == ['events', 0, 0, None, 'hello', 42]
    api_server.join()
