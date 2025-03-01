from dataclasses import dataclass
from typing import Optional, Dict, Any
import uuid

from .....seedwork.application.commands import Command, CommandHandler
from .....seedwork.infrastructure.uow import UnitOfWork
from ...domain.entities import AnonymizationTask
from ...domain.value_objects import ImageType, AnonymizationStatus
from ...infrastructure.messaging.pulsar_publisher import PulsarPublisher

@dataclass
class RouteToAnonymizer(Command):
    """Comando para enrutar una imagen al servicio de anonimización correspondiente"""
    image_id: uuid.UUID
    task_id: uuid.UUID
    image_type: ImageType
    source: str
    modality: str
    region: str
    file_path: str

@dataclass
class CompleteAnonymization(Command):
    """Comando para marcar una tarea de anonimización como completada"""
    task_id: uuid.UUID
    result_file_path: str
    processing_time_ms: int = 0

@dataclass
class FailAnonymization(Command):
    """Comando para marcar una tarea de anonimización como fallida"""
    task_id: uuid.UUID
    error_message: str

class UoWRouteToAnonymizerHandler(CommandHandler):
    """Manejador para el comando RouteToAnonymizer usando UoW"""
    
    def __init__(self, uow: UnitOfWork, publisher: PulsarPublisher):
        self.uow = uow
        self.publisher = publisher

    async def handle(self, command: RouteToAnonymizer) -> Dict[str, Any]:
        async with self.uow:
            try:
                # Obtener repositorio
                repository = self.uow.repository('anonymization_task')
                
                # Crear tarea de anonimización
                task = AnonymizationTask(
                    id=uuid.uuid4(),
                    image_id=command.image_id,
                    task_id=command.task_id,
                    image_type=command.image_type,
                    source=command.source,
                    modality=command.modality,
                    region=command.region,
                    file_path=command.file_path
                )
                
                # Enrutar la tarea al servicio correspondiente
                routing_event = task.route_to_anonymizer()
                
                # Guardar la tarea
                await repository.save(task)
                
                # Publicar evento de solicitud de anonimización
                await self.publisher.publish_event(routing_event)
                
                # Confirmar transacción
                await self.uow.commit()
                
                return {
                    "task_id": str(task.id),
                    "image_id": str(task.image_id),
                    "destination_service": routing_event.destination_service,
                    "status": "ROUTED"
                }
                
            except Exception as e:
                # No es necesario hacer rollback explícito ya que el context manager lo hará
                raise e

class UoWCompleteAnonymizationHandler(CommandHandler):
    """Manejador para el comando CompleteAnonymization usando UoW"""
    
    def __init__(self, uow: UnitOfWork, publisher: PulsarPublisher):
        self.uow = uow
        self.publisher = publisher

    async def handle(self, command: CompleteAnonymization) -> Dict[str, Any]:
        async with self.uow:
            try:
                # Obtener repositorio
                repository = self.uow.repository('anonymization_task')
                
                # Obtener la tarea
                task = await repository.get_by_id(command.task_id)
                if not task:
                    raise ValueError(f"No se encontró la tarea con ID: {command.task_id}")
                
                # Completar anonimización
                complete_event = task.complete_anonymization(
                    result_file_path=command.result_file_path,
                    processing_time_ms=command.processing_time_ms
                )
                
                # Publicar eventos
                for event in task.events:
                    await self.publisher.publish_event(event)
                
                # Actualizar la tarea
                await repository.update(task)
                
                # Confirmar transacción
                await self.uow.commit()
                
                return {
                    "task_id": str(task.id),
                    "image_id": str(task.image_id),
                    "status": "COMPLETED",
                    "result_file_path": task.result_file_path
                }
                
            except Exception as e:
                # No es necesario hacer rollback explícito ya que el context manager lo hará
                raise e

class UoWFailAnonymizationHandler(CommandHandler):
    """Manejador para el comando FailAnonymization usando UoW"""
    
    def __init__(self, uow: UnitOfWork, publisher: PulsarPublisher):
        self.uow = uow
        self.publisher = publisher

    async def handle(self, command: FailAnonymization) -> Dict[str, Any]:
        async with self.uow:
            try:
                # Obtener repositorio
                repository = self.uow.repository('anonymization_task')
                
                # Obtener la tarea
                task = await repository.get_by_id(command.task_id)
                if not task:
                    raise ValueError(f"No se encontró la tarea con ID: {command.task_id}")
                
                # Marcar como fallida
                fail_event = task.fail_anonymization(error_message=command.error_message)
                
                # Publicar evento
                await self.publisher.publish_event(fail_event)
                
                # Actualizar la tarea
                await repository.update(task)
                
                # Confirmar transacción
                await self.uow.commit()
                
                return {
                    "task_id": str(task.id),
                    "image_id": str(task.image_id),
                    "status": "FAILED",
                    "error_message": task.error_message
                }
                
            except Exception as e:
                # No es necesario hacer rollback explícito ya que el context manager lo hará
                raise e

# Funciones para ayudar a ejecutar los comandos
async def uow_route_to_anonymizer(
    handler: UoWRouteToAnonymizerHandler,
    image_id: uuid.UUID,
    task_id: uuid.UUID,
    image_type: ImageType,
    source: str,
    modality: str,
    region: str,
    file_path: str
) -> Dict[str, Any]:
    """Ejecuta el comando RouteToAnonymizer"""
    command = RouteToAnonymizer(
        image_id=image_id,
        task_id=task_id,
        image_type=image_type,
        source=source,
        modality=modality,
        region=region,
        file_path=file_path
    )
    return await handler.handle(command)

async def uow_complete_anonymization(
    handler: UoWCompleteAnonymizationHandler,
    task_id: uuid.UUID,
    result_file_path: str,
    processing_time_ms: int = 0
) -> Dict[str, Any]:
    """Ejecuta el comando CompleteAnonymization"""
    command = CompleteAnonymization(
        task_id=task_id,
        result_file_path=result_file_path,
        processing_time_ms=processing_time_ms
    )
    return await handler.handle(command)

async def uow_fail_anonymization(
    handler: UoWFailAnonymizationHandler,
    task_id: uuid.UUID,
    error_message: str
) -> Dict[str, Any]:
    """Ejecuta el comando FailAnonymization"""
    command = FailAnonymization(
        task_id=task_id,
        error_message=error_message
    )
    return await handler.handle(command)