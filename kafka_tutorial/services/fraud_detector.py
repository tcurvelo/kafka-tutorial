import time

from kafka_tutorial import settings
from kafka_tutorial.order import key_deserializer, value_deserializer
from kafka_tutorial.services import KafkaService


class FraudDetectorService(KafkaService):
    def __init__(self, *args, **kwargs):

        super().__init__(
            topics=[settings.ECOMMERCE_NEW_ORDER],
            extras=["fraud_detector"],
            properties={
                "key.deserializer": key_deserializer,
                "value.deserializer": value_deserializer,
            },
        )

    def parse(self, msg):
        print(
            "🕵️ Processing new order, checking for fraud: "
            f"{msg.key()}: {msg.value()} ({msg.partition()}, {msg.offset()})"
        )
        time.sleep(5)
        print("Order processed")
        print("------------------------------------------")


if __name__ == "__main__":
    FraudDetectorService()()
