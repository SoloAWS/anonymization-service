import logging
import asyncio
from typing import Dict
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config.settings import get_settings
from .config.database import init_db, create_session
from .config.dependencies import setup_dependencies
from .modules.anonymization.infrastructure.messaging.pulsar_publisher import PulsarPublisher
from .modules.anonymization.infrastructure.messaging.pulsar_consumer import PulsarConsumer
from .modules.anonymization.application.events.event_handlers import (
    ImageReadyForAnonymizationHandler,
    AnonymizationCompletedHandler,
    AnonymizationFailedHandler
)
from .api import api_router
from .modules.anonymization.domain.events import (
    ImageReadyForAnonymization,
    AnonymizationCompleted, 
    AnonymizationFailed
)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Obtener configuración
settings = get_settings()

# Crear la aplicación FastAPI
app = FastAPI(
    title="Anonymization Service",
    description="Servicio para gestión de anonimización de imágenes médicas",
    version="1.0.0"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configurar rutas de API
app.include_router(api_router, prefix="/api")

# Mapeo de eventos a temas de Pulsar
PULSAR_PUBLISHER_TOPICS_MAPPING: Dict[str, str] = {
    "AnonymizationRequested": "persistent://public/default/anonymization-requests",
    "AnonymizationCompleted": "persistent://public/default/anonymization-completed",
    "AnonymizationFailed": "persistent://public/default/anonymization-failed",
    "ImageReadyForProcessing": "persistent://public/default/image-processing"
}

PULSAR_CONSUMER_TOPICS_MAPPING: Dict[str, str] = {
    "ImageReadyForAnonymization": "persistent://public/default/image-anonymization",
    "AnonymizationHistCompleted": "persistent://public/default/histology-anonymization-completed",
    "AnonymizationXrayCompleted": "persistent://public/default/xray-anonymization-completed",
    "AnonymizationMriCompleted": "persistent://public/default/mri-anonymization-completed",
    "AnonymizationHistFailed": "persistent://public/default/histology-anonymization-failed",
    "AnonymizationXrayFailed": "persistent://public/default/xray-anonymization-failed",
    "AnonymizationMriFailed": "persistent://public/default/mri-anonymization-failed"
}

# Variables para guardar las instancias de mensajería
publisher = None
consumer = None

# Inicialización de la aplicación
@app.on_event("startup")
async def startup_event():
    logger.info("Iniciando servicio de anonimización")
    
    global publisher, consumer
    
    # Inicializar la base de datos
    try:
        await init_db()
        logger.info("Base de datos inicializada correctamente")
    except Exception as e:
        logger.error(f"Error al inicializar la base de datos: {str(e)}")
        raise
    
    # Inicializar el publicador de Pulsar
    try:
        publisher = PulsarPublisher(
            service_url=settings.pulsar_service_url,
            topics_mapping=PULSAR_PUBLISHER_TOPICS_MAPPING
        )
        
        # Configurar dependencias
        setup_dependencies(publisher)
        logger.info("Publicador de Pulsar inicializado correctamente")
    except Exception as e:
        logger.error(f"Error al inicializar el publicador de Pulsar: {str(e)}")
        logger.warning("Continuando sin publicador de Pulsar configurado")
    
    # Inicializar el consumidor de Pulsar
    try:
        consumer = PulsarConsumer(
            service_url=settings.pulsar_service_url,
            topics_mapping=PULSAR_CONSUMER_TOPICS_MAPPING
        )
        logger.info(f"Listening to consumer topics: {PULSAR_CONSUMER_TOPICS_MAPPING}")

        # Inicializar manejadores de eventos
        from .config.dependencies import get_anonymization_task_repository
        
        # Crear una sesión para los manejadores
        session = create_session()
        
        # Crear repositorio para los manejadores
        repository = get_anonymization_task_repository(session)
        
        # Registrar manejadores
        image_ready_handler = ImageReadyForAnonymizationHandler(repository, publisher)
        anonymization_completed_handler = AnonymizationCompletedHandler(repository, publisher)
        anonymization_failed_handler = AnonymizationFailedHandler(repository, publisher)
        
        # Registrar manejadores de eventos
        consumer.register_event_handler(
            "ImageReadyForAnonymization", 
            lambda event: image_ready_handler.handle(event)
        )
        consumer.register_event_handler(
            "AnonymizationHistCompleted", 
            lambda event: anonymization_completed_handler.handle(event)
        )
        consumer.register_event_handler(
            "AnonymizationXrayCompleted", 
            lambda event: anonymization_completed_handler.handle(event)
        )
        consumer.register_event_handler(
            "AnonymizationMriCompleted", 
            lambda event: anonymization_completed_handler.handle(event)
        )
        consumer.register_event_handler(
            "AnonymizationHistFailed", 
            lambda event: anonymization_failed_handler.handle(event)
        )
        consumer.register_event_handler(
            "AnonymizationXrayFailed", 
            lambda event: anonymization_failed_handler.handle(event)
        )
        consumer.register_event_handler(
            "AnonymizationMriFailed", 
            lambda event: anonymization_failed_handler.handle(event)
        )
        
        # Iniciar la escucha de mensajes
        await consumer.start_listening()
        logger.info("Consumidor de Pulsar inicializado correctamente")
    except Exception as e:
        logger.error(f"Error al inicializar el consumidor de Pulsar: {str(e)}")
        logger.warning("Continuando sin consumidor de Pulsar configurado")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Cerrando servicio de anonimización")
    global publisher, consumer
    
    # Detener consumidor
    if consumer:
        await consumer.stop()
    
    # Cerrar publicador
    if publisher:
        publisher.close()
    
# Ruta por defecto
@app.get("/")
async def root():
    return {"message": "Anonymization Service API"}

# Endpoint de health check
@app.get("/anonymization/health", tags=["health"])
async def health_check():
    """Endpoint para verificar el estado del servicio"""
    return {
        "status": "ok",
        "service": "anonymization-service",
        "version": "1.0.0"
    }

# Configuración para ejecutar directamente
if __name__ == "__main__":
    import uvicorn
    
    settings = get_settings()
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
        log_level=settings.log_level.lower()
    )