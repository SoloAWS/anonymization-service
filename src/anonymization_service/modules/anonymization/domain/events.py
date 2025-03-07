from dataclasses import dataclass, field
from datetime import datetime
import uuid
from typing import Optional

from ....seedwork.domain.events import DomainEvent
from .value_objects import ImageType, AnonymizationStatus

@dataclass
class ImageReadyForAnonymization(DomainEvent):
    """Evento que indica que una imagen está lista para anonimización"""
    image_id: uuid.UUID = field(default=None)
    task_id: uuid.UUID = field(default=None)
    source: str = field(default=None)
    modality: str = field(default=None)
    region: str = field(default=None)
    file_path: str = field(default=None)
    image_type: ImageType = field(default=ImageType.UNKNOWN)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "image_id": str(self.image_id),
            "task_id": str(self.task_id),
            "source": self.source,
            "modality": self.modality,
            "region": self.region,
            "file_path": self.file_path,
            "image_type": self.image_type.value
        }

@dataclass
class AnonymizationRequested(DomainEvent):
    """Evento que indica que se ha solicitado anonimización para una imagen"""
    image_id: uuid.UUID = field(default=None)
    task_id: uuid.UUID = field(default=None)
    image_type: ImageType = field(default=ImageType.UNKNOWN)
    source: str = field(default=None)
    modality: str = field(default=None)
    region: str = field(default=None)
    file_path: str = field(default=None)
    destination_service: str = field(default=None)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "image_id": str(self.image_id),
            "task_id": str(self.task_id),
            "image_type": self.image_type.value,
            "source": self.source,
            "modality": self.modality,
            "region": self.region,
            "file_path": self.file_path,
            "destination_service": self.destination_service
        }

@dataclass
class AnonymizationCompleted(DomainEvent):
    """Evento que indica que se ha completado la anonimización"""
    image_id: uuid.UUID = field(default=None)
    task_id: uuid.UUID = field(default=None)
    image_type: ImageType = field(default=ImageType.UNKNOWN)
    result_file_path: str = field(default=None)
    processing_time_ms: int = field(default=0)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "image_id": str(self.image_id),
            "task_id": str(self.task_id),
            "image_type": self.image_type.value,
            "result_file_path": self.result_file_path,
            "processing_time_ms": self.processing_time_ms
        }

@dataclass
class AnonymizationFailed(DomainEvent):
    """Evento que indica que ha fallado la anonimización"""
    image_id: uuid.UUID = field(default=None)
    task_id: uuid.UUID = field(default=None)
    image_type: ImageType = field(default=ImageType.UNKNOWN)
    error_message: str = field(default=None)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "image_id": str(self.image_id),
            "task_id": str(self.task_id),
            "image_type": self.image_type.value,
            "error_message": self.error_message
        }

@dataclass
class ImageReadyForProcessing(DomainEvent):
    """Evento que indica que una imagen está lista para procesamiento tras anonimización"""
    image_id: uuid.UUID = field(default=None)
    task_id: uuid.UUID = field(default=None)
    image_type: ImageType = field(default=ImageType.UNKNOWN)
    anonymized_file_path: str = field(default=None)
    original_file_path: str = field(default=None)
    source: str = field(default=None)
    modality: str = field(default=None)
    region: str = field(default=None)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "image_id": str(self.image_id),
            "task_id": str(self.task_id),
            "image_type": self.image_type.value,
            "anonymized_file_path": self.anonymized_file_path,
            "original_file_path": self.original_file_path,
            "source": self.source,
            "modality": self.modality,
            "region": self.region
        }
        
@dataclass
class AnonymizationRolledBack(DomainEvent):
    """Evento que indica que se ha revertido una anonimización por compensación"""
    task_id: uuid.UUID = field(default=None)
    image_id: uuid.UUID = field(default=None)
    reason: str = field(default=None)
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "task_id": str(self.task_id),
            "image_id": str(self.image_id) if self.image_id else None,
            "reason": self.reason
        }