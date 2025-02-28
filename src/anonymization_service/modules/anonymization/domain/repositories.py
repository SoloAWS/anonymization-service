from abc import ABC, abstractmethod
from typing import List, Optional
import uuid

from .entities import AnonymizationTask

class AnonymizationTaskRepository(ABC):
    """Interfaz para el repositorio de tareas de anonimización"""
    
    @abstractmethod
    async def get_by_id(self, task_id: uuid.UUID) -> Optional[AnonymizationTask]:
        """Obtiene una tarea de anonimización por su ID"""
        pass

    @abstractmethod
    async def save(self, task: AnonymizationTask) -> None:
        """Guarda una tarea de anonimización"""
        pass

    @abstractmethod
    async def update(self, task: AnonymizationTask) -> None:
        """Actualiza una tarea de anonimización existente"""
        pass
    
    @abstractmethod
    async def get_pending_tasks(self) -> List[AnonymizationTask]:
        """Obtiene las tareas pendientes"""
        pass
    
    @abstractmethod
    async def get_tasks_by_image_id(self, image_id: uuid.UUID) -> List[AnonymizationTask]:
        """Obtiene tareas asociadas a una imagen"""
        pass