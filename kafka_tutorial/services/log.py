from kafka_tutorial import settings
from kafka_tutorial.services import KafkaService


class LogService(KafkaService):
    def __init__(self, *args, **kwargs):
        super().__init__(
            settings.Topic.ECOMMERCE_ALL,
            *args,
            extras=["email"],
            **kwargs,
        )

    def parse(self, msg):
        print(
            f"✨ [{msg.topic()}]\t{msg.key().decode()}:\tpartition:{msg.partition()}\toffset:{msg.offset()}"
            f"\n  {msg.value().decode()}"
            f"\n{'-'*100}"
        )


if __name__ == "__main__":
    LogService()()
