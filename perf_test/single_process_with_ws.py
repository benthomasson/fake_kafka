#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:
    single_process_with_api [options]

Options:
    -h, --help          Show this page
    --debug             Show debug logging
    --verbose           Show verbose logging
    -n=<i>              n [default: 1000000]
    --partition=<p>     Partition
    --key=<k>           Key
    --csv1=<c1>         producer results csv [default: single-process-with-api-producer.csv]
    --csv2=<c2>         consumer results csv [default: single-process-with-api-consumer.csv]
"""
from docopt import docopt
import logging
import sys
import time
import fake_kafka
import asyncio
import csv
import threading
import requests

import fake_kafka.api

from uvicorn.config import Config
from uvicorn.main import Server

logger = logging.getLogger('single_process')


async def produce_messages(loop, n, key, partition, csv1):
    producer = fake_kafka.AIOKafkaProducer(loop=loop, bootstrap_servers=['http://127.0.0.1:8000'], use_websocket=True)
    await producer.start()
    if partition is not None:
        partition = int(partition)
    start = time.time()
    for i in range(n):
        await producer.send_and_wait("my_topic", "Super message", key=key, partition=partition)
    end = time.time()
    print("{} messages sent in {} s for {} ns/m or {} m/s".format(n, end - start, (end - start) * 1000000 / n, int(n / (end - start))))
    await producer.stop()
    with open(csv1, 'a') as f:
        writer = csv.writer(f)
        writer.writerow([n, end - start, (end - start) * 1000000 / n, int(n / (end - start))])


async def consume_messages(loop, n, key, partition, csv2):
    consumer = fake_kafka.AIOKafkaConsumer("my_topic", loop=loop, bootstrap_servers=['http://127.0.0.1:8000'], group_id='a', use_websocket=True)
    await consumer.start()
    start = time.time()
    count = 0
    try:
        async for msg in consumer:
            count += 1
            if count >= n:
                break
        assert count == n, 'Did not receive expected number of messages'
        end = time.time()
    finally:
        await consumer.stop()
    print("{} messages recieved in {} s for {} ns/m or {} m/s".format(n, end - start, (end - start) * 1000000 / n, int(n / (end - start))))
    with open(csv2, 'a') as f:
        writer = csv.writer(f)
        writer.writerow([n, end - start, (end - start) * 1000000 / n, int(n / (end - start))])


def start_server(limit_max_requests):
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


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    parsed_args = docopt(__doc__, args)
    if parsed_args['--debug']:
        logging.basicConfig(level=logging.DEBUG)
    elif parsed_args['--verbose']:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    _, server = start_server(3)
    loop = asyncio.get_event_loop()

    print('single_process n: {} key: {} partition: {}'.format(parsed_args['-n'], parsed_args['--key'], parsed_args['--partition']))
    loop.run_until_complete(produce_messages(loop, int(parsed_args['-n']), parsed_args['--key'], parsed_args['--partition'], parsed_args['--csv1']))
    loop.run_until_complete(consume_messages(loop, int(parsed_args['-n']), parsed_args['--key'], parsed_args['--partition'], parsed_args['--csv2']))
    server.force_exit = True
    requests.get("http://127.0.0.1:8000/docs")
    loop.close()
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
