import logging
from typing import Dict, Type
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ..seedwork.infrastructure.uow import SqlAlchemyUnitOfWork
from ..seedwork.domain.repositories import Repository
from ..config.database import get_db, create_session
from ..modules.anonymization.domain.repositories import AnonymizationTaskRepository
from ..modules.anonymization.infrastructure.persistence.repositories import SQLAnonymizationTaskRepository
from ..modules.anonymization.infrastructure.messaging.pulsar_publisher import PulsarPublisher

logger = logging.getLogger(__name__)

_publisher_instance = None

def setup_dependencies(publisher: PulsarPublisher):
    """Set up the dependencies with the initialized publisher"""
    global _publisher_instance
    _publisher_instance = publisher
    logger.info("Dependencies initialized with publisher")

def get_publisher():
    """Returns the PulsarPublisher singleton"""
    return _publisher_instance

# Repository factories
def get_anonymization_task_repository(db: AsyncSession = Depends(get_db)):
    """Returns an AnonymizationTaskRepository implementation"""
    return SQLAnonymizationTaskRepository(db)

# UnitOfWork related dependencies
def get_repository_factories() -> Dict[str, Type[Repository]]:
    """Returns a dictionary of repository factories for UoW"""
    return {
        'anonymization_task': SQLAnonymizationTaskRepository
    }

def get_unit_of_work():
    """Returns a new SqlAlchemyUnitOfWork instance"""
    repositories_factories = get_repository_factories()
    return SqlAlchemyUnitOfWork(create_session, repositories_factories)