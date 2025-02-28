from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class Command:
    """Base command class"""

    pass


class CommandHandler(ABC):
    @abstractmethod
    async def handle(self, command: Command) -> Any:
        """Handle the command"""
        pass
