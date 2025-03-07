from dataclasses import dataclass
import logging
import os
import uuid
from typing import Dict, Any, Optional

from .....seedwork.application.commands import Command, CommandHandler
from .....seedwork.infrastructure.uow import UnitOfWork
from ...domain.repositories import AnonymizationTaskRepository
from ...domain.value_objects import AnonymizationStatus
from ...domain.entities import AnonymizationTask
from ..events.event_handlers import AnonymizationFailed
from ...infrastructure.messaging.pulsar_publisher import PulsarPublisher

logger = logging.getLogger(__name__)

@dataclass
class RollbackAnonymizationCommand(Command):
    """Comando para revertir una anonimización como compensación"""
    task_id: uuid.UUID
    reason: str = "Compensación de saga"

class RollbackAnonymizationHandler(CommandHandler):
    """Manejador para el comando RollbackAnonymization"""
    
    def __init__(self, uow: UnitOfWork, publisher: PulsarPublisher):
        self.uow = uow
        self.publisher = publisher

    async def handle(self, command: RollbackAnonymizationCommand) -> Dict[str, Any]:
        async with self.uow:
            try:
                # Obtener repositorio
                repository = self.uow.repository('anonymization_task')
                
                # Obtener la tarea
                task = await repository.get_by_id(command.task_id)
                if not task:
                    raise ValueError(f"No se encontró la tarea de anonimización con ID: {command.task_id}")
                
                # 1. Eliminar el archivo anonimizado si existe
                if task.result_file_path and os.path.exists(task.result_file_path):
                    try:
                        os.remove(task.result_file_path)
                        logger.info(f"Archivo anonimizado eliminado: {task.result_file_path}")
                    except Exception as e:
                        logger.error(f"Error al eliminar archivo anonimizado {task.result_file_path}: {str(e)}")
                
                # 2. Marcar la tarea como fallida
                fail_event = task.fail_anonymization(
                    error_message=f"Anonimización revertida: {command.reason}"
                )
                
                # 3. Actualizar la tarea en la base de datos
                await repository.update(task)
                
                # 4. Publicar evento de fallo
                self.publisher.publish_event(fail_event)
                
                # Confirmar transacción
                await self.uow.commit()
                
                return {
                    "task_id": str(command.task_id),
                    "image_id": str(task.image_id) if task.image_id else None,
                    "status": "ROLLBACK_COMPLETED",
                    "reason": command.reason
                }
                
            except Exception as e:
                logger.error(f"Error al revertir anonimización: {str(e)}")
                raise e


# Funciones para ayudar a ejecutar el comando
async def uow_rollback_anonymization(
    handler: RollbackAnonymizationHandler,
    task_id: uuid.UUID,
    reason: str = "Compensación de saga"
) -> Dict[str, Any]:
    """Ejecuta el comando RollbackAnonymization"""
    command = RollbackAnonymizationCommand(
        task_id=task_id,
        reason=reason
    )
    return await handler.handle(command)