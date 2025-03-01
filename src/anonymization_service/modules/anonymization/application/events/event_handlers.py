import asyncio
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
        
        try:
            # En lugar de enrutar, procesamos la anonimización directamente
            # Simular tiempo de procesamiento
            await asyncio.sleep(1)
            
            # Completar anonimización y generar evento
            processing_event = task.complete_anonymization(
                result_file_path="result_file_path",
                processing_time_ms=1000  # Tiempo simulado
            )
            
            # Guardar la tarea en el repositorio
            await self.repository.save(task)
            
            # Publicar eventos (incluyendo ImageReadyForProcessing)
            for event in task.events:
                await self.publisher.publish_event(event)
            
            logger.info(
                f"Tarea de anonimización completada: {task.id}. "
                f"Imagen: {event.image_id}, Tipo: {image_type.value}"
            )
            
        except Exception as e:
            # En caso de error, marcamos la tarea como fallida
            logger.error(f"Error al procesar anonimización: {str(e)}")
            
            task.fail_anonymization(error_message=f"Error al procesar anonimización: {str(e)}")
            
            # Guardar la tarea en el repositorio
            await self.repository.save(task)
            
            # Publicar evento de fallo
            for event in task.events:
                await self.publisher.publish_event(event)