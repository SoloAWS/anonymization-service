from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
import uuid
from pydantic import BaseModel, Field
from enum import Enum
import logging

from ...modules.anonymization.application.commands.uow_commands import (
    UoWRouteToAnonymizerHandler,
    UoWCompleteAnonymizationHandler,
    UoWFailAnonymizationHandler,
    uow_route_to_anonymizer,
    uow_complete_anonymization,
    uow_fail_anonymization
)

from ...modules.anonymization.domain.value_objects import ImageType
from ...config.dependencies import get_publisher, get_unit_of_work

# Configurar logging
logger = logging.getLogger(__name__)

# Crear router
router = APIRouter()

# Modelos de Pydantic para la API
class ImageTypeEnum(str, Enum):
    HISTOLOGY = "HISTOLOGY"
    XRAY = "XRAY"
    MRI = "MRI"
    UNKNOWN = "UNKNOWN"

class RouteImageRequest(BaseModel):
    image_id: str
    task_id: str
    image_type: ImageTypeEnum
    source: str
    modality: str
    region: str
    file_path: str

class CompleteAnonymizationRequest(BaseModel):
    result_file_path: str
    processing_time_ms: int = 0

class FailAnonymizationRequest(BaseModel):
    error_message: str

class StatusResponse(BaseModel):
    status: str
    message: str

# Endpoints
@router.post("/route", response_model=Dict[str, Any])
async def route_image(
    request: RouteImageRequest,
    publisher = Depends(get_publisher),
    uow = Depends(get_unit_of_work)
):
    """Enruta una imagen al servicio de anonimización correspondiente"""
    try:
        # Crear handler con UoW
        handler = UoWRouteToAnonymizerHandler(
            uow,
            publisher
        )
        
        # Ejecutar comando usando UoW
        result = await uow_route_to_anonymizer(
            handler=handler,
            image_id=uuid.UUID(request.image_id),
            task_id=uuid.UUID(request.task_id),
            image_type=ImageType[request.image_type.value],
            source=request.source,
            modality=request.modality,
            region=request.region,
            file_path=request.file_path
        )
        
        return result
    except Exception as e:
        logger.error(f"Error al enrutar imagen: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/tasks/{task_id}/complete", response_model=Dict[str, Any])
async def complete_anonymization(
    task_id: str,
    request: CompleteAnonymizationRequest,
    publisher = Depends(get_publisher),
    uow = Depends(get_unit_of_work)
):
    """Marca una tarea de anonimización como completada"""
    try:
        # Crear handler con UoW
        handler = UoWCompleteAnonymizationHandler(
            uow,
            publisher
        )
        
        # Ejecutar comando usando UoW
        result = await uow_complete_anonymization(
            handler=handler,
            task_id=uuid.UUID(task_id),
            result_file_path=request.result_file_path,
            processing_time_ms=request.processing_time_ms
        )
        
        return result
    except ValueError as e:
        if "No se encontró la tarea" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al completar anonimización: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/tasks/{task_id}/fail", response_model=Dict[str, Any])
async def fail_anonymization(
    task_id: str,
    request: FailAnonymizationRequest,
    publisher = Depends(get_publisher),
    uow = Depends(get_unit_of_work)
):
    """Marca una tarea de anonimización como fallida"""
    try:
        # Crear handler con UoW
        handler = UoWFailAnonymizationHandler(
            uow,
            publisher
        )
        
        # Ejecutar comando usando UoW
        result = await uow_fail_anonymization(
            handler=handler,
            task_id=uuid.UUID(task_id),
            error_message=request.error_message
        )
        
        return result
    except ValueError as e:
        if "No se encontró la tarea" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al marcar anonimización como fallida: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health", response_model=StatusResponse)
async def health_check():
    """Endpoint para verificar el estado del servicio"""
    return {
        "status": "ok",
        "message": "Anonymization service is running"
    }