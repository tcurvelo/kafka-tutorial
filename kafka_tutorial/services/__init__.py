from typing import Any

from confluent_kafka import DeserializingConsumer, SerializingProducer
from kafka_tutorial import settings
from kafka_tutorial.utils import key_serializer, value_serializer


class KafkaService:
    def __init__(self, topic: settings.Topic, parse=None, extras=None, properties=None):
        config = {
            **settings.get_config(extras=extras),
            **(properties if properties else {}),
        }
        self.consumer = DeserializingConsumer(config)
        self.consumer.subscribe(topics=[topic.value])
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


def delivery_callback(err, msg):
    if err:
        print("ERROR: Message failed delivery: {}".format(err))
    else:
        print(f"🚀 [{msg.topic()}]\t{msg.key().decode()}")


class KafkaDispatcher:
    def __init__(self):
        self.producer = SerializingProducer(
            {
                **settings.get_config(),
                "value.serializer": value_serializer,
                "key.serializer": key_serializer,
            }
        )

    def __del__(self):
        self.producer.flush()

    def send(self, topic: settings.Topic, key, value):
        self.producer.produce(topic.value, key, value, on_delivery=delivery_callback)
