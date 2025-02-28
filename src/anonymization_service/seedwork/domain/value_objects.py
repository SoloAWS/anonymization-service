from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass(frozen=True)
class ValueObject:
    def __eq__(self, other):
        if not isinstance(other, ValueObject):
            return False
        return self.__dict__ == other.__dict__
