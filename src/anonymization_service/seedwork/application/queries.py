from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass
class Query:
    """Base query class"""

    pass


@dataclass
class QueryResult(Generic[T]):
    result: T


class QueryHandler(ABC):
    @abstractmethod
    async def handle(self, query: Query) -> QueryResult:
        """Handle the query"""
        pass
