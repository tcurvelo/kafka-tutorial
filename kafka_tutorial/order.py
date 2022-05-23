import dataclasses
import json
from dataclasses import dataclass
from uuid import UUID


@dataclass
class Order:
    user_id: UUID
    order_id: UUID
    amount: int
