from configparser import ConfigParser
from enum import Enum


class Topic(Enum):
    ECOMMERCE_ALL = "^ECOMMERCE_.*"
    ECOMMERCE_NEW_ORDER = "ECOMMERCE_NEW_ORDER"
    ECOMMERCE_SEND_EMAIL = "ECOMMERCE_SEND_EMAIL"
    ECOMMERCE_ORDER_APROVED = "ECOMMERCE_ORDER_APROVED"
    ECOMMERCE_ORDER_REJECTED = "ECOMMERCE_ORDER_REJECTED"

    @classmethod
    def list_valid_topics(cls):
        return [e.value for e in cls if not e.value.startswith("^")]


def get_config(extras=None):
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    with open("config.ini") as f:
        config_parser = ConfigParser()
        config_parser.read_file(f)
        config = dict(config_parser["default"])
        if extras:
            for section in extras:
                config.update(dict(config_parser[section]))

    return config


if __name__ == "__main__":
    print("\n".join(Topic.list_valid_topics()))
