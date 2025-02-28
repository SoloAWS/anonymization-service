from abc import ABC, abstractmethod
from typing import Dict, Type, TypeVar, Any, Optional
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from ..domain.repositories import Repository

logger = logging.getLogger(__name__)

T = TypeVar('T')

class UnitOfWork(ABC):
    """
    Abstracción para el patrón Unit of Work.
    Define la interfaz para manejar transacciones atómicas en la capa de persistencia.
    """
    
    @abstractmethod
    async def __aenter__(self):
        """Inicia una transacción y retorna el unit of work"""
        pass
        
    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Finaliza la transacción, haciendo rollback si ocurre una excepción"""
        pass
        
    @abstractmethod
    async def commit(self):
        """Confirma los cambios en la transacción actual"""
        pass
        
    @abstractmethod
    async def rollback(self):
        """Descarta los cambios en la transacción actual"""
        pass


class SqlAlchemyUnitOfWork(UnitOfWork):
    """
    Implementación concreta del Unit of Work para SQLAlchemy.
    Gestiona las transacciones y proporciona acceso a los repositorios
    dentro del contexto de una sesión compartida.
    """
    
    def __init__(self, session_factory, repositories_factory: Dict[str, Type[Repository]]):
        """
        Inicializa el Unit of Work con una fábrica de sesiones y fábricas de repositorios.
        
        Args:
            session_factory: Fábrica que crea sesiones de SQLAlchemy
            repositories_factory: Diccionario de fábricas de repositorios
        """
        self.session_factory = session_factory
        self.repositories_factory = repositories_factory
        self._repositories = {}
        self._session = None
    
    async def __aenter__(self):
        """
        Inicia una nueva transacción y prepara el UoW para su uso.
        
        Returns:
            El propio UoW para permitir el uso del patrón 'async with'
        """
        self._session = self.session_factory()
        self._repositories = {}
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Finaliza la transacción actual. Si hay una excepción, hace rollback;
        si no, intenta hacer commit si flush_only es False.
        
        Args:
            exc_type: Tipo de excepción si ocurrió alguna
            exc_val: Valor de la excepción si ocurrió alguna
            exc_tb: Traceback de la excepción si ocurrió alguna
        """
        try:
            if exc_type:
                # Si ocurre una excepción, hacemos rollback automáticamente
                await self.rollback()
                logger.warning(f"Transaction rolled back due to exception: {exc_val}")
            
            # Cerramos la sesión en cualquier caso
            await self._session.close()
            self._session = None
            self._repositories = {}
            
        except Exception as e:
            logger.error(f"Error during UoW exit: {e}")
            # Intentamos cerrar la sesión si aún existe
            if self._session:
                await self._session.close()
                self._session = None
            self._repositories = {}
            # Re-lanzamos la excepción para que se maneje en un nivel superior
            raise
    
    async def commit(self):
        """
        Confirma los cambios en la transacción actual.
        Lanza una excepción si ocurre algún error durante el commit.
        """
        if not self._session:
            raise ValueError("Cannot commit - no active session")
        
        try:
            await self._session.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            logger.error(f"Error during commit: {e}")
            await self.rollback()
            raise
    
    async def rollback(self):
        """
        Descarta los cambios en la transacción actual.
        """
        if not self._session:
            return
            
        try:
            await self._session.rollback()
            logger.debug("Transaction rolled back")
        except Exception as e:
            logger.error(f"Error during rollback: {e}")
            raise
    
    def _get_repository(self, name: str) -> Repository:
        """
        Obtiene o crea un repositorio del tipo especificado utilizando la sesión actual.
        
        Args:
            name: Nombre del repositorio a obtener
            
        Returns:
            Una instancia del repositorio solicitado
            
        Raises:
            KeyError: Si el repositorio no está registrado
        """
        if name not in self.repositories_factory:
            raise KeyError(f"Repository '{name}' not registered in UoW")
            
        if name not in self._repositories:
            factory = self.repositories_factory[name]
            self._repositories[name] = factory(self._session)
            
        return self._repositories[name]
    
    def repository(self, name: str) -> Repository:
        """
        Método público para obtener un repositorio por su nombre.
        
        Args:
            name: Nombre del repositorio a obtener
            
        Returns:
            Una instancia del repositorio solicitado
        """
        return self._get_repository(name)