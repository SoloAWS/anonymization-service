from dataclasses import dataclass, field
from datetime import datetime
import uuid
from abc import ABC, abstractmethod


@dataclass
class Entity:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def __eq__(self, other):
        if not isinstance(other, Entity):
            return False
        return self.id == other.id
