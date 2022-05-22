import dataclasses
import json
from dataclasses import dataclass
from uuid import UUID


@dataclass
class Order:
    user_id: UUID
    order_id: UUID
    amount: int


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        if isinstance(o, UUID):
            return o.hex
        return super().default(o)


def serializer(obj, context) -> bytes:
    return json.dumps(obj, cls=EnhancedJSONEncoder).encode("utf-8")


def deserializer(context, data):
    return Order(json.loads(data.decode("utf-8")))
