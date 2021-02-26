#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'aiohttp==3.7.4',
    'async-timeout==3.0.1',
    'attrs==19.1.0',
    'chardet==3.0.4',
    'click==7.0',
    'dataclasses==0.6',
    'docopt==0.6.2',
    'fastapi==0.38.1',
    'h11==0.8.1',
    'httptools==0.0.13',
    'idna-ssl==1.1.0',
    'idna==2.8',
    'multidict==4.5.2',
    'pydantic==0.32.2',
    'starlette==0.12.8',
    'typing-extensions==3.7.4',
    'uvicorn==0.9.0',
    'uvloop==0.13.0',
    'websockets==8.0.2',
    'yarl==1.3.0',
]

setup_requirements = ['pytest-runner']

test_requirements = ['pytest']

setup(
    author="Ben Thomasson",
    author_email='ben.thomasson@gmail.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="A test double for Kafka using asyncio.",
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='fake_kafka',
    name='fake_kafka',
    packages=find_packages(include=['fake_kafka']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/benthomasson/fake_kafka',
    version='0.1.0',
    zip_safe=False,
)
