from dataclasses import dataclass, field
from .entities import Entity
from .events import DomainEvent
from typing import List


@dataclass
class AggregateRoot(Entity):
    events: List[DomainEvent] = field(default_factory=list)

    def add_event(self, event: DomainEvent):
        self.events.append(event)

    def clear_events(self):
        self.events = []
