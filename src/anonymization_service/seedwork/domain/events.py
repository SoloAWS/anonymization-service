from dataclasses import dataclass, field
from datetime import datetime
import uuid


@dataclass
class DomainEvent:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        return {"id": str(self.id), "timestamp": self.timestamp.isoformat()}
