#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:
    consumer [options]

Options:
    -h, --help         Show this page
    --debug            Show debug logging
    --verbose          Show verbose logging
    --address=<a>      Fake kafa address [Default: http://localhost:9092]
    --group=<g>        Group [Default: a]
"""
from docopt import docopt
import logging
import sys
import asyncio

from fake_kafka import AIOKafkaConsumer

logger = logging.getLogger('consumer')


async def run_consumer(parsed_args):
    consumer = AIOKafkaConsumer(bootstrap_servers=[parsed_args['--address']],
                                topic="events",
                                group_id=parsed_args['--group'],
                                use_websocket=True)
    await consumer.start()
    print('started')
    try:
        print('waiting')
        async for message in consumer:
            print(message)
    finally:
        await consumer.stop()
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
