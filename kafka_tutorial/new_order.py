from random import randint
from uuid import uuid4

from confluent_kafka import SerializingProducer
from kafka_tutorial import settings
from kafka_tutorial.order import Order, serializer


def delivery_callback(err, msg):
    if err:
        print("ERROR: Message failed delivery: {}".format(err))
    else:
        print(f"🚀 [{msg.topic()}]\t{msg.key().decode()}: {msg.value().decode()}")


def main():
    producer = SerializingProducer(
        {**settings.get_config(), "value.serializer": serializer}
    )
    for i in range(10):
        user_id = uuid4()
        order = Order(user_id=user_id, order_id=uuid4(), amount=randint(1, 5000))

        producer.produce(
            settings.ECOMMERCE_NEW_ORDER,
            user_id.hex,
            order,
            on_delivery=delivery_callback,
        )

        producer.produce(
            settings.ECOMMERCE_SEND_EMAIL,
            key=user_id.hex,
            value="Thank you for your order! We are processing your order!",
            on_delivery=delivery_callback,
        )

    producer.poll(1)
    producer.flush()


if __name__ == "__main__":
    main()
