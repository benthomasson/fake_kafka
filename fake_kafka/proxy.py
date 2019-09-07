

import aiohttp
import time
import logging

from fake_kafka.messages import FakeKafkaMessage
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


class FakeKafkaServerProxy:

    def __init__(self, address, use_websocket=False):
        self.address = address
        self.session = aiohttp.ClientSession()
        self.use_websocket = use_websocket
        self.consumer_websocket = None
        self.producer_websocket = None

    async def consumer_subscribe(self, consumer, topic, group_id):
        await self.session.post(urljoin(self.address, '/subscription'),
                                json=dict(consumer_id=consumer.consumer_id,
                                          topic=topic,
                                          group_id=group_id))

    async def get(self, consumer, topic):
        if self.use_websocket and self.consumer_websocket is None:
            self.consumer_websocket = await self.session.ws_connect(urljoin(self.address,
                                                                            '/consumer_topic_message_ws/{}/{}'.format(topic, consumer.consumer_id)))

            logger.debug('connected consumer_websocket')
        if self.use_websocket:
            try:
                data = await self.consumer_websocket.receive_json()
                logger.debug('data %s', data)
                return data
            except TypeError as e:
                logger.error(e)
        else:
            response = await self.session.get(urljoin(self.address,
                                                      '/topic_message/{topic}/{consumer_id}'.format(topic=topic,
                                                                                                    consumer_id=consumer.consumer_id)))
            return await response.json()

    async def send(self, topic, message):
        if self.use_websocket and self.producer_websocket is None:
            self.producer_websocket = await self.session.ws_connect(urljoin(self.address,
                                                                            '/producer_topic_message_ws'))

            logger.debug('connected producer_websocket')
        if self.use_websocket:
            await self.producer_websocket.send_json(dict(topic=topic,
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

    async def all_partitions(self, topic):
        response = await self.session.get(urljoin(self.address,
                                                  '/all_partitions/{topic}'.format(topic=topic)))
        return await response.json()

    async def seek(self, consumer, topic, partition, offset):

        response = await self.session.post(urljoin(self.address,
                                                   "/topic_partition_offset/"),
                                           json=dict(consumer_id=consumer.consumer_id,
                                                     topic=topic,
                                                     partition=partition,
                                                     offset=offset))
        return

    async def close(self):
        if self.producer_websocket is not None:
            await self.producer_websocket.close()
        if self.consumer_websocket is not None:
            await self.consumer_websocket.close()
        if self.session is not None:
            await self.session.close()
