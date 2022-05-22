from confluent_kafka import DeserializingConsumer
from kafka_tutorial import settings


def main():
    consumer = DeserializingConsumer(settings.get_config(extras=["log"]))
    consumer.subscribe(topics=[settings.ECOMMERCE_ALL])

    try:
        while True:
            if (msg := consumer.poll(3)) is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                print(
                    f"✨ [{msg.topic()}]\t{msg.key().decode()}:\tpartition:{msg.partition()}\toffset:{msg.offset()}"
                    f"\n  {msg.value().decode()}"
                    f"\n{'-'*100}"
                )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
