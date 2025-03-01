import logging
import uuid
from typing import Dict, Any

from .....seedwork.application.events import EventHandler
from ...domain.events import (
    ImageReadyForAnonymization, 
    AnonymizationCompleted, 
    AnonymizationFailed
)
from ...domain.entities import AnonymizationTask
from ...domain.value_objects import ImageType

logger = logging.getLogger(__name__)

class ImageReadyForAnonymizationHandler(EventHandler):
    """
    Manejador para el evento ImageReadyForAnonymization.
    Procesa las imágenes que están listas para anonimización.
    """
    
    def __init__(self, repository, publisher):
        self.repository = repository
        self.publisher = publisher
    
    async def handle(self, event: ImageReadyForAnonymization):
        logger.info(
            f"Recibida imagen {event.image_id} para anonimización. "
            f"Tarea: {event.task_id}, "
            f"Tipo: {event.image_type.value if hasattr(event, 'image_type') else 'Desconocido'}, "
            f"Fuente: {event.source}, "
            f"Modalidad: {event.modality}, "
            f"Región: {event.region}, "
            f"Ruta: {event.file_path}"
        )
        
        # Determinar el tipo de imagen basado en la modalidad
        image_type = ImageType.UNKNOWN
        if event.modality.upper() == "HISTOLOGY" or "HIST" in event.modality.upper():
            image_type = ImageType.HISTOLOGY
        elif event.modality.upper() == "XRAY" or "RAY" in event.modality.upper():
            image_type = ImageType.XRAY
        elif event.modality.upper() == "MRI" or "MAGNETIC" in event.modality.upper():
            image_type = ImageType.MRI
        
        # Crear tarea de anonimización
        task = AnonymizationTask(
            id=uuid.uuid4(),
            image_id=event.image_id,
            task_id=event.task_id,
            image_type=image_type,
            source=event.source,
            modality=event.modality,
            region=event.region,
            file_path=event.file_path
        )
        
        # Enrutar la tarea al servicio de anonimización correspondiente
        routing_event = task.route_to_anonymizer()
        
        # Guardar la tarea en el repositorio
        await self.repository.save(task)
        
        # Publicar evento de solicitud de anonimización
        await self.publisher.publish_event(routing_event)
        
        logger.info(
            f"Tarea de anonimización creada y enrutada: {task.id}. "
            f"Imagen: {event.image_id}, Tipo: {image_type.value}, "
            f"Destino: {routing_event.destination_service}"
        )

class AnonymizationCompletedHandler(EventHandler):
    """
    Manejador para el evento AnonymizationCompleted.
    Procesa la confirmación de anonimización completada.
    """
    
    def __init__(self, repository, publisher):
        self.repository = repository
        self.publisher = publisher
    
    async def handle(self, event: AnonymizationCompleted):
        logger.info(
            f"Anonimización completada para imagen {event.image_id}. "
            f"Tarea: {event.task_id}, "
            f"Tipo: {event.image_type.value}, "
            f"Tiempo de procesamiento: {event.processing_time_ms}ms, "
            f"Ruta del resultado: {event.result_file_path}"
        )
        
        # Buscar la tarea en el repositorio
        task = await self.repository.get_by_id(event.task_id)
        
        if task:
            # Actualizar la tarea con la información de completado
            processing_event = task.complete_anonymization(
                result_file_path=event.result_file_path,
                processing_time_ms=event.processing_time_ms
            )
            
            # Actualizar la tarea en el repositorio
            await self.repository.update(task)
            
            # Publicar evento de imagen lista para procesamiento
            await self.publisher.publish_event(processing_event)
            
            logger.info(f"Tarea de anonimización actualizada como completada: {task.id}")
        else:
            logger.warning(f"No se encontró la tarea de anonimización: {event.task_id}")

class AnonymizationFailedHandler(EventHandler):
    """
    Manejador para el evento AnonymizationFailed.
    Procesa los fallos en la anonimización.
    """
    
    def __init__(self, repository, publisher):
        self.repository = repository
        self.publisher = publisher
    
    async def handle(self, event: AnonymizationFailed):
        logger.error(
            f"Anonimización fallida para imagen {event.image_id}. "
            f"Tarea: {event.task_id}, "
            f"Tipo: {event.image_type.value}, "
            f"Error: {event.error_message}"
        )
        
        # Buscar la tarea en el repositorio
        task = await self.repository.get_by_id(event.task_id)
        
        if task:
            # Actualizar la tarea con la información de fallo
            task.fail_anonymization(error_message=event.error_message)
            
            # Actualizar la tarea en el repositorio
            await self.repository.update(task)
            
            logger.info(f"Tarea de anonimización actualizada como fallida: {task.id}")
        else:
            logger.warning(f"No se encontró la tarea de anonimización: {event.task_id}")