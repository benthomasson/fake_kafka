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
* Extremely fast performance in single process mode at 100000 messages per second ingress and egress
* Multiple process mode using a REST API with moderate performance at 1000 messages per second ingress and egress
* Multiple process mode using a REST API and websockets with fast performance at 10000 messages per second ingress and egress
* Support for consumer groups
* Open API Specification v3 using FastAPI


Installation
------------

To install fake_kafka follow these steps::

    $ git clone https://github.com/benthomasson/fake_kafka.git
    $ cd fake_kafka
    $ python setup.py install


Usage
-----

Using local python::

    $ ./examples/server &
    $ open http://localhost:9092/docs


Demonstration
-------------

Using local python::

    $ ./examples/server &
    $ ./examples/consumer.py &
    $ ./examples/producer.py


Using docker::

    $ docker-compose build
    $ docker-compose up


Performance Tests
-----------------

Using local python::

    $ cd perf_test
    $ ./single_process.py -n 100000
    $ ./single_process_with_api.py -n 10000
    $ ./single_process_with_ws.py -n 100000


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
