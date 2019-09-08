==========
Fake Kafka
==========

.. image:: https://coveralls.io/repos/github/benthomasson/fake_kafka/badge.svg
        :target: https://coveralls.io/github/benthomasson/fake_kafka

.. image:: https://img.shields.io/travis/benthomasson/fake_kafka.svg
        :target: https://travis-ci.org/benthomasson/fake_kafka


A test double for Kafka using asyncio.

* Free software: Apache Software License 2.0

Features
--------

* Support for a subset of aiokafka's API
* Fully event driven implementation using Asyncio
* Single process mode which is useful inside tests
* Extremely fast performance in single process mode
* Consumer, Producer, Server mode using a REST API
* Consumer, Producer, Server mode using a REST API and websockets for very fast performance
* Support for consumer groups
* Open API Specification v3 using FastAPI


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
