#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:
    single_process [options]

Options:
    -h, --help          Show this page
    --debug             Show debug logging
    --verbose           Show verbose logging
    -n=<i>              n [default: 1000000]
    --partition=<p>     Partition
    --key=<k>           Key
    --csv1=<c1>         producer results csv [default: single-process-producer.csv]
    --csv2=<c2>         consumer results csv [default: single-process-consumer.csv]
"""
from docopt import docopt
import logging
import sys
import time
import fake_kafka
import asyncio
import csv

logger = logging.getLogger('single_process')


async def produce_messages(loop, n, key, partition, csv1):
    producer = fake_kafka.AIOKafkaProducer(loop=loop)
    await producer.start()
    if partition is not None:
        partition = int(partition)
    start = time.time()
    for i in range(n):
        await producer.send_and_wait("my_topic", b"Super message", key=key, partition=partition)
    end = time.time()
    print("{} messages sent in {} s for {} ns/m or {} m/s".format(n, end - start, (end - start) * 1000000 / n, int(n / (end - start))))
    await producer.stop()
    with open(csv1, 'a') as f:
        writer = csv.writer(f)
        writer.writerow([n, end - start, (end - start) * 1000000 / n, int(n / (end - start))])


async def consume_messages(loop, n, key, partition, csv2):
    consumer = fake_kafka.AIOKafkaConsumer("my_topic", loop=loop)
    await consumer.start()
    start = time.time()
    count = 0
    try:
        async for msg in consumer:
            count += 1
            pass
        end = time.time()
    finally:
        await consumer.stop()
    print("{} messages recieved in {} s for {} ns/m or {} m/s".format(n, end - start, (end - start) * 1000000 / n, int(n / (end - start))))
    with open(csv2, 'a') as f:
        writer = csv.writer(f)
        writer.writerow([n, end - start, (end - start) * 1000000 / n, int(n / (end - start))])


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

    loop = asyncio.get_event_loop()

    print('single_process n: {} key: {} partition: {}'.format(parsed_args['-n'], parsed_args['--key'], parsed_args['--partition']))
    loop.run_until_complete(produce_messages(loop, int(parsed_args['-n']), parsed_args['--key'], parsed_args['--partition'], parsed_args['--csv1']))
    loop.run_until_complete(consume_messages(loop, int(parsed_args['-n']), parsed_args['--key'], parsed_args['--partition'], parsed_args['--csv2']))
    loop.close()
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
