import time

from kafka_tutorial import settings
from kafka_tutorial.order import key_deserializer, value_deserializer
from kafka_tutorial.services import KafkaDispatcher, KafkaService


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
        self.dispatcher = KafkaDispatcher()

    def parse(self, msg):
        print(
            "🕵️ Processing new order, checking for fraud: "
            f"{msg.key()}: {msg.value()} ({msg.partition()}, {msg.offset()})"
        )
        time.sleep(5)

        order = msg.value()
        if self.is_fraud(order):
            print("💀 Order is a fraud!")
            self.dispatcher.send(
                settings.Topic.ECOMMERCE_ORDER_REJECTED, order.user_id, order
            )
        else:
            print("✔️ Order processed")
            self.dispatcher.send(settings.ECOMMERCE_ORDER_APROVED, order.user_id, order)

        print("------------------------------------------")

    def is_fraud(self, order):
        # pretending that the fraud happens when amount > 4500
        return order.amount > 4500


if __name__ == "__main__":
    FraudDetectorService()()
