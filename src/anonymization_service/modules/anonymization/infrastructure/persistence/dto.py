from sqlalchemy import Column, String, JSON, DateTime, Boolean
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime

from .....config.database import Base

class AnonymizationTaskDTO(Base):
    """DTO para la entidad AnonymizationTask"""
    __tablename__ = "anonymization_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True)
    image_id = Column(String)
    task_id = Column(String)
    image_type = Column(String, nullable=False)
    source = Column(String)
    modality = Column(String)
    region = Column(String)
    file_path = Column(String)
    result_file_path = Column(String)
    status = Column(String, nullable=False, default="PENDING")
    error_message = Column(String)
    _metadata = Column(JSON)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)