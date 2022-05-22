from configparser import ConfigParser

ECOMMERCE_NEW_ORDER = "ECOMMERCE_NEW_ORDER"
ECOMMERCE_SEND_EMAIL = "ECOMMERCE_SEND_EMAIL"
ECOMMERCE_ALL = "^ECOMMERCE_.*"


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
