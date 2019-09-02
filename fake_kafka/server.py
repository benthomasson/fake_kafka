

from collections import defaultdict


# Singleton FakeKafkaServer
class FakeKafkaServer:

    __instance = None

    def __new__(cls):
        if FakeKafkaServer.__instance is None:
            FakeKafkaServer.__instance = object.__new__(cls)
            FakeKafkaServer._init_topics()
        return FakeKafkaServer.__instance

    @classmethod
    def _init_topics(cls):
        cls.__instance.topics = defaultdict(list)

    def send(self, topic, message):
        self.topics[topic].append(message)

    def get(self, topic, index):
        if index < len(self.topics[topic]):
            return self.topics[topic][index]
        else:
            return None
