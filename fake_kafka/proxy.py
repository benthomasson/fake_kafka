

import aiohttp
import time

from fake_kafka.messages import FakeKafkaMessage
from urllib.parse import urljoin


class FakeKafkaServerProxy:

    def __init__(self, address):
        self.address = address
        self.session = aiohttp.ClientSession()

    async def consumer_subscribe(self, consumer, topic, group_id):
        await self.session.post(urljoin(self.address, '/subscription'),
                                json=dict(consumer_id=consumer.consumer_id,
                                          topic=topic,
                                          group_id=group_id))

    async def get(self, consumer, topic):
        response = await self.session.get(urljoin(self.address,
                                              '/topic_message/{topic}/{consumer_id}'.format(topic=topic,
                                                                                            consumer_id=consumer.consumer_id)))
        return await response.json()

    async def send(self, topic, message):
        return await self.session.post(urljoin(self.address,
                                              '/topic_message/'),
                                       json=dict(topic=topic,
                                                 partition=message.partition,
                                                 key=message.key,
                                                 value=message.value,
                                                 timestamp=message.timestamp))

    async def all_partitions(self, topic):
        response = await self.session.get(urljoin(self.address,
                                              '/all_partitions/{topic}'.format(topic=topic)))
        return await response.json()
