#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:
    producer [options]

Options:
    -h, --help         Show this page
    --debug            Show debug logging
    --verbose          Show verbose logging
    -n=<n>             Number of messages to send [Default: 10]
    --partition=<p>     Partition
    --key=<k>           Key
    --address=<a>      Fake kafa address [Default: http://localhost:9092]
"""
from docopt import docopt
import logging
import sys
import asyncio
import os

from fake_kafka import AIOKafkaProducer

logger = logging.getLogger('producer')

async def run_consumer(parsed_args):
    producer = AIOKafkaProducer(bootstrap_servers=[parsed_args['--address']],
                                use_websocket=True)
    await producer.start()
    print('started')
    try:
        message = "Hello from {}".format(os.getpid())
        for i in range(int(parsed_args['-n'])):
            await producer.send_and_wait("events", message, key=parsed_args['--key'], partition=parsed_args['--partition'])
            print('.', end='')
    finally:
        await producer.stop()
        print('stopped')


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
    loop.run_until_complete(run_consumer(parsed_args))
    loop.close()
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

