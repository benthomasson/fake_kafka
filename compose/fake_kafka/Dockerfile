FROM python:3.6-alpine

ENV PYTHONUNBUFFERED 1

RUN apk update \
  && apk add --virtual build-deps gcc python3-dev musl-dev \
  && apk add alpine-sdk

COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt
COPY . /tmp/fake_kafka
RUN pip install /tmp/fake_kafka

COPY ./compose/fake_kafka/start /start
RUN sed -i 's/\r//' /start
RUN chmod +x /start

