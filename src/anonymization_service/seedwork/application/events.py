from abc import ABC, abstractmethod
from typing import List
from ..domain.events import DomainEvent


class EventHandler(ABC):
    @abstractmethod
    async def handle(self, event: DomainEvent):
        """Handle the domain event"""
        pass
