import time

from kafka_tutorial import settings
from kafka_tutorial.services import KafkaService


class EmailService(KafkaService):
    def __init__(self):
        super().__init__(
            topic=settings.Topic.ECOMMERCE_SEND_EMAIL,
            extras=["email"],
        )

    def parse(self, msg):
        print(
            "📬 Sending email: "
            f"{msg.key().decode()}: "
            f"{msg.value().decode()} ({msg.partition()}, {msg.offset()})"
        )
        time.sleep(2)
        print("Email sent")


if __name__ == "__main__":
    EmailService()()
