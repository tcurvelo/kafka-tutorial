from typing import Any

from confluent_kafka import DeserializingConsumer
from kafka_tutorial import settings


class KafkaService:
    def __init__(self, topics, parse=None, extras=None, properties=None):
        config = {
            **settings.get_config(extras=extras),
            **(properties if properties else {}),
        }
        self.consumer = DeserializingConsumer(config)
        self.consumer.subscribe(topics=topics)
        if not hasattr(self, "parse"):
            self.parse = parse

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        try:
            while True:
                if (msg := self.consumer.poll(3)) is None:
                    print("Waiting...")
                elif msg.error():
                    print("ERROR: %s".format(msg.error()))
                else:
                    self.parse(msg)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
