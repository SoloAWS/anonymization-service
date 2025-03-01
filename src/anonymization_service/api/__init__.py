# API package initialization
from fastapi import APIRouter

# Import routers from versioned API modules
from .v1 import router as v1_router

# Create main API router
api_router = APIRouter()

# Include versioned routers
api_router.include_router(v1_router, prefix="/v1")