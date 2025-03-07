import logging
import uuid
from typing import Dict, Any, Optional

from .....seedwork.infrastructure.uow import UnitOfWork
from ...infrastructure.messaging.pulsar_publisher import PulsarPublisher
from .compensation_commands import RollbackAnonymizationHandler, uow_rollback_anonymization

logger = logging.getLogger(__name__)

# Diccionario para mapear tipos de comandos a sus manejadores
command_handlers = {}

async def handle_rollback_anonymization(
    command_data: Dict[str, Any],
    uow: UnitOfWork,
    publisher: PulsarPublisher,
    correlation_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Manejador para el comando RollbackAnonymization recibido via Pulsar.
    
    Args:
        command_data: Datos del comando
        uow: Unidad de trabajo para transacciones
        publisher: Publicador de eventos
        correlation_id: ID de correlación opcional
        
    Returns:
        Dict: Resultado de la operación
    """
    logger.info(f"Processing RollbackAnonymization command: {command_data}")
    
    try:
        # Extraer datos del comando
        task_id_str = command_data.get('task_id')
        reason = command_data.get('reason', "Compensación de saga")
        
        # Validar datos requeridos
        if not task_id_str:
            raise ValueError("Missing required command data fields: task_id")
        
        # Convertir ID a UUID
        try:
            task_id = uuid.UUID(task_id_str)
        except ValueError:
            raise ValueError(f"Invalid task_id format: {task_id_str}")
        
        # Crear handler
        handler = RollbackAnonymizationHandler(uow, publisher)
        
        # Ejecutar comando
        result = await uow_rollback_anonymization(
            handler=handler,
            task_id=task_id,
            reason=reason
        )
        
        # Añadir correlation_id a la respuesta
        if correlation_id:
            result["correlation_id"] = correlation_id
        
        return result
    except Exception as e:
        logger.error(f"Error handling RollbackAnonymization command: {str(e)}")
        raise

# Registrar el manejador
command_handlers["RollbackAnonymization"] = handle_rollback_anonymization