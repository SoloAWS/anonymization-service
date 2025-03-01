from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from contextlib import asynccontextmanager
import logging

from .settings import get_settings

# Obtener configuración
settings = get_settings()

# Configurar logging
logger = logging.getLogger(__name__)

# Crear el motor de base de datos
engine = create_async_engine(
    settings.db_url,
    echo=settings.environment == "dev",
    future=True
)

# Crear la sesión asíncrona
async_session_factory = sessionmaker(
    engine, 
    class_=AsyncSession, 
    expire_on_commit=False
)

Base = declarative_base()

async def init_db():
    """Inicializa la base de datos"""    
    
    async with engine.begin() as conn:
        # En desarrollo, se pueden eliminar y recrear las tablas
        # if settings.environment == "dev":
        #    logger.info("Dropping all tables in development mode")
        #    await conn.run_sync(Base.metadata.drop_all)
            
        logger.info("Creating all tables")
        await conn.run_sync(Base.metadata.create_all)
        
    logger.info("Database initialized successfully")

# Función para obtener una sesión de base de datos
# Esta función se mantiene para compatibilidad con código existente y endpoints
# que no han sido migrados al patrón UoW
@asynccontextmanager
async def get_db():
    """Provides an async database session"""
    async with async_session_factory() as session:
        try:
            yield session
        except Exception as e:
            await session.rollback()
            logger.error(f"Error with database session: {e}")
            raise

# Función para crear una nueva sesión de base de datos
# Esta función se utiliza principalmente por el UnitOfWork
def create_session() -> AsyncSession:
    """Creates a new database session"""
    return async_session_factory()