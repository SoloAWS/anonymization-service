from enum import Enum
from dataclasses import dataclass, field
from typing import Optional
from ....seedwork.domain.value_objects import ValueObject

class ImageType(Enum):
    HISTOLOGY = "HISTOLOGY"
    XRAY = "XRAY"
    MRI = "MRI"
    UNKNOWN = "UNKNOWN"

class AnonymizationStatus(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

@dataclass(frozen=True)
class ImageMetadata(ValueObject):
    """Metadatos asociados a una imagen médica"""
    image_type: ImageType
    modality: str
    region: str
    size_bytes: int
    dimensions: Optional[str] = None

@dataclass(frozen=True)
class AnonymizationResult(ValueObject):
    """Resultado de un proceso de anonimización"""
    status: AnonymizationStatus
    message: str
    processing_time_ms: int = 0
    details: Optional[dict] = None