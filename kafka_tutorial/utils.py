import dataclasses
import json
from uuid import UUID


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        if isinstance(o, UUID):
            return o.hex
        return super().default(o)


def value_serializer(obj, context) -> bytes:
    return json.dumps(obj, cls=EnhancedJSONEncoder).encode("utf-8")


def value_deserializer(data, context):
    return json.loads(data.decode("utf-8"))


def key_serializer(obj, context) -> bytes:
    key = obj.hex if isinstance(obj, UUID) else obj
    return key.encode("utf-8")


def key_deserializer(data, context):
    return UUID(hex=data.decode("utf-8"))
