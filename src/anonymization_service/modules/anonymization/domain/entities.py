from dataclasses import dataclass, field
from datetime import datetime
import uuid
from typing import Optional

from ....seedwork.domain.entities import Entity
from ....seedwork.domain.aggregate import AggregateRoot
from .value_objects import ImageType, AnonymizationStatus, ImageMetadata, AnonymizationResult
from .events import AnonymizationRequested, AnonymizationCompleted, AnonymizationFailed, ImageReadyForProcessing, AnonymizationRolledBack

@dataclass
class AnonymizationTask(AggregateRoot):
    """
    Representa una tarea de anonimización de una imagen médica.
    """
    image_id: uuid.UUID = field(default=None)
    task_id: uuid.UUID = field(default=None)
    image_type: ImageType = field(default=ImageType.UNKNOWN)
    source: str = field(default=None)
    modality: str = field(default=None)
    region: str = field(default=None)
    file_path: str = field(default=None)
    result_file_path: Optional[str] = field(default=None)
    status: AnonymizationStatus = field(default=AnonymizationStatus.PENDING)
    metadata: dict = field(default_factory=dict)
    started_at: Optional[datetime] = field(default=None)
    completed_at: Optional[datetime] = field(default=None)
    error_message: Optional[str] = field(default=None)
    
    def __post_init__(self):
        if not hasattr(self, 'events'):
            self.events = []
    
    def route_to_anonymizer(self) -> AnonymizationRequested:
        """
        Este método ya no es necesario en el nuevo flujo, pero lo mantenemos 
        por compatibilidad con el código existente.
        """
        self.status = AnonymizationStatus.IN_PROGRESS
        self.started_at = datetime.now()
        
        # Ya no determinamos un servicio de destino específico
        destination_service = "anonymization-service"
        
        # Crear evento de solicitud de anonimización
        event = AnonymizationRequested(
            image_id=self.image_id,
            task_id=self.task_id,
            image_type=self.image_type,
            source=self.source,
            modality=self.modality,
            region=self.region,
            file_path=self.file_path,
            destination_service=destination_service
        )
        
        self.add_event(event)
        return event
    
    def complete_anonymization(self, result_file_path: str, processing_time_ms: int = 0) -> AnonymizationCompleted:
        """
        Marca la tarea de anonimización como completada.
        """
        self.status = AnonymizationStatus.COMPLETED
        self.completed_at = datetime.now()
        self.result_file_path = result_file_path
        
        # Crear evento de anonimización completada
        complete_event = AnonymizationCompleted(
            image_id=self.image_id,
            task_id=self.task_id,
            image_type=self.image_type,
            result_file_path=result_file_path,
            processing_time_ms=processing_time_ms
        )
        
        # Crear evento de imagen lista para procesamiento
        processing_event = ImageReadyForProcessing(
            image_id=self.image_id,
            task_id=self.task_id,
            image_type=self.image_type,
            anonymized_file_path=result_file_path,
            original_file_path=self.file_path,
            source=self.source,
            modality=self.modality,
            region=self.region
        )
        
        self.add_event(complete_event)
        self.add_event(processing_event)
        return complete_event
    
    def fail_anonymization(self, error_message: str) -> AnonymizationFailed:
        """
        Marca la tarea de anonimización como fallida.
        """
        self.status = AnonymizationStatus.FAILED
        self.completed_at = datetime.now()
        self.error_message = error_message
        
        # Crear evento de anonimización fallida
        event = AnonymizationFailed(
            image_id=self.image_id,
            task_id=self.task_id,
            image_type=self.image_type,
            error_message=error_message
        )
        
        self.add_event(event)
        return event
    
    def rollback_anonymization(self, reason: str) -> AnonymizationRolledBack:
        """
        Marca la tarea de anonimización como revertida por compensación.
        """
        self.status = AnonymizationStatus.FAILED
        self.completed_at = datetime.now()
        self.error_message = f"Anonimización revertida por compensación: {reason}"
        
        # Crear evento de anonimización revertida
        event = AnonymizationRolledBack(
            task_id=self.id,
            image_id=self.image_id,
            reason=reason
        )
        
        self.add_event(event)
        return event