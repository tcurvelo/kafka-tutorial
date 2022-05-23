from random import randint
from uuid import uuid4

from kafka_tutorial.order import Order
from kafka_tutorial.services import KafkaDispatcher
from kafka_tutorial.settings import Topic


def main():
    # with KafkaDispatcher() as order_dispatcher, KafkaDispatcher() as email_dispatcher:
    order_dispatcher = KafkaDispatcher()
    email_dispatcher = KafkaDispatcher()
    for i in range(10):
        user_id = uuid4()

        order = Order(user_id=user_id, order_id=uuid4(), amount=randint(1, 5000))
        order_dispatcher.send(Topic.ECOMMERCE_NEW_ORDER, user_id, order)

        msg = "Thank you for your order! We are processing your order!"
        email_dispatcher.send(Topic.ECOMMERCE_SEND_EMAIL, user_id, msg)


if __name__ == "__main__":
    main()
