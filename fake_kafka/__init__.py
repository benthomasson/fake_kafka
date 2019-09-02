# -*- coding: utf-8 -*-

"""Top-level package for FakeKafka."""

__author__ = """Ben Thomasson"""
__email__ = 'ben.thomasson@gmail.com'
__version__ = '0.1.0'


from .client import AIOKafkaProducer, AIOKafkaConsumer
from .server import FakeKafkaServer

__all__ = [AIOKafkaProducer.__name__,
           AIOKafkaConsumer.__name__,
           FakeKafkaServer.__name__]
