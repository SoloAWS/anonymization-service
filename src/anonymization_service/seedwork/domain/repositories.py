from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List, Optional

T = TypeVar('T')

class Repository(Generic[T], ABC):
    """
    Interfaz base para los repositorios.
    Define operaciones comunes para todos los repositorios.
    """
    
    @abstractmethod
    async def get_by_id(self, id) -> Optional[T]:
        """Obtiene una entidad por su ID"""
        pass
        
    @abstractmethod
    async def save(self, entity: T) -> None:
        """Guarda una entidad"""
        pass
        
    @abstractmethod
    async def update(self, entity: T) -> None:
        """Actualiza una entidad existente"""
        pass