import os
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache

class Settings(BaseSettings):
    """Configuración global de la aplicación"""
    
    # Configuración general
    environment: str = Field(default="dev")
    log_level: str = Field(default="INFO")
    
    # Configuración de la base de datos
    db_host: str = Field(default="localhost")
    db_port: int = Field(default=5433)
    db_user: str = Field(default="user")
    db_password: str = Field(default="password")
    db_name: str = Field(default="anonymization_db")
    
    @property
    def db_url(self) -> str:
        """URL de conexión a la base de datos"""
        return os.getenv("DATABASE_URL", f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}")
    
    # Configuración de Pulsar
    pulsar_service_url: str = Field(default="pulsar://localhost:6650")
    
    # Configuración del API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8001)
    api_reload: bool = Field(default=True)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

@lru_cache()
def get_settings() -> Settings:
    """Retorna una instancia de la configuración cacheada"""
    return Settings()