from dataclasses import dataclass
from typing import Any


@dataclass
class VolatileValue:
    value: Any
    expiration: int
