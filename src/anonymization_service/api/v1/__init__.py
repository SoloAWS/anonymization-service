# Version 1 API initialization
from fastapi import APIRouter

# Import routers
from .anonymization import router as anonymization_router

# Create v1 router
router = APIRouter()

# Include service-specific routers
router.include_router(anonymization_router, prefix="/anonymization", tags=["Anonymization"])