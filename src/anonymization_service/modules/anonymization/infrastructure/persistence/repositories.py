import datetime
from typing import List, Optional
import uuid
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from ...domain.entities import AnonymizationTask
from ...domain.repositories import AnonymizationTaskRepository
from ...domain.value_objects import ImageType, AnonymizationStatus

from .dto import AnonymizationTaskDTO

class SQLAnonymizationTaskRepository(AnonymizationTaskRepository):
    """Implementación de AnonymizationTaskRepository con SQLAlchemy"""
    
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, task_id: uuid.UUID) -> Optional[AnonymizationTask]:
        """Obtiene una tarea de anonimización por su ID"""
        dto = await self.session.get(AnonymizationTaskDTO, task_id)
        if not dto:
            return None
        return self._dto_to_entity(dto)

    async def save(self, task: AnonymizationTask) -> None:
        """Guarda una tarea de anonimización"""
        # Verificar si la tarea ya existe
        existing_task = await self.session.get(AnonymizationTaskDTO, task.id)
        
        if existing_task:
            # Si la tarea existe, actualizarla en lugar de crear una nueva
            await self.update(task)
        else:
            # Si no existe, crear una nueva tarea
            dto = self._entity_to_dto(task)
            self.session.add(dto)
            await self.session.commit()

    async def update(self, task: AnonymizationTask) -> None:
        """Actualiza una tarea de anonimización existente"""
        # Obtener tarea existente
        existing_task = await self.session.get(AnonymizationTaskDTO, task.id)
        if not existing_task:
            raise ValueError(f"No se puede actualizar una tarea que no existe: {task.id}")
        
        # Actualizar la tarea existente con los nuevos valores
        existing_task.image_id = str(task.image_id)
        existing_task.task_id = str(task.task_id)
        existing_task.image_type = task.image_type.value
        existing_task.source = task.source
        existing_task.modality = task.modality
        existing_task.region = task.region
        existing_task.file_path = task.file_path
        existing_task.result_file_path = task.result_file_path
        existing_task.status = task.status.value
        existing_task._metadata = task.metadata
        existing_task.error_message = task.error_message
        existing_task.started_at = task.started_at
        existing_task.completed_at = task.completed_at
        existing_task.updated_at = datetime.datetime.now()
        
        # No es necesario añadir el DTO a la sesión ya que ya está siendo rastreado
        await self.session.commit()

    async def get_pending_tasks(self) -> List[AnonymizationTask]:
        """Obtiene las tareas pendientes"""
        query = (
            select(AnonymizationTaskDTO)
            .filter(AnonymizationTaskDTO.status == AnonymizationStatus.PENDING.value)
            .order_by(desc(AnonymizationTaskDTO.created_at))
        )
        result = await self.session.execute(query)
        dtos = result.scalars().all()
        return [self._dto_to_entity(dto) for dto in dtos]

    async def get_tasks_by_image_id(self, image_id: uuid.UUID) -> List[AnonymizationTask]:
        """Obtiene tareas asociadas a una imagen"""
        query = (
            select(AnonymizationTaskDTO)
            .filter(AnonymizationTaskDTO.image_id == str(image_id))
            .order_by(desc(AnonymizationTaskDTO.created_at))
        )
        result = await self.session.execute(query)
        dtos = result.scalars().all()
        return [self._dto_to_entity(dto) for dto in dtos]

    def _dto_to_entity(self, dto: AnonymizationTaskDTO) -> AnonymizationTask:
        """Convierte un DTO a una entidad de dominio"""
        return AnonymizationTask(
            id=dto.id,
            image_id=uuid.UUID(dto.image_id) if dto.image_id else None,
            task_id=uuid.UUID(dto.task_id) if dto.task_id else None,
            image_type=ImageType(dto.image_type),
            source=dto.source,
            modality=dto.modality,
            region=dto.region,
            file_path=dto.file_path,
            result_file_path=dto.result_file_path,
            status=AnonymizationStatus(dto.status),
            metadata=dto._metadata or {},
            error_message=dto.error_message,
            started_at=dto.started_at,
            completed_at=dto.completed_at,
            created_at=dto.created_at,
            updated_at=dto.updated_at
        )

    def _entity_to_dto(self, entity: AnonymizationTask) -> AnonymizationTaskDTO:
        """Convierte una entidad de dominio a un DTO"""
        return AnonymizationTaskDTO(
            id=entity.id,
            image_id=str(entity.image_id) if entity.image_id else None,
            task_id=str(entity.task_id) if entity.task_id else None,
            image_type=entity.image_type.value,
            source=entity.source,
            modality=entity.modality,
            region=entity.region,
            file_path=entity.file_path,
            result_file_path=entity.result_file_path,
            status=entity.status.value,
            _metadata=entity.metadata,
            error_message=entity.error_message,
            started_at=entity.started_at,
            completed_at=entity.completed_at,
            created_at=entity.created_at,
            updated_at=entity.updated_at
        )