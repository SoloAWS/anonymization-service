import logging
import asyncio
import json
import uuid
from datetime import datetime

import pulsar
from .....modules.anonymization.domain.events import AnonymizationRequested
from .....modules.anonymization.domain.value_objects import ImageType
from ...application.events.event_handlers import HistologyAnonymizationHandler

logger = logging.getLogger(__name__)

class HistologyAnonymizerConsumer:
    """Consumidor específico para el servicio de anonimización de histología"""
    
    def __init__(self, service_url, publisher, topic="persistent://public/default/anonymization-requests"):
        self.service_url = service_url
        self.topic = topic
        self.subscription_name = "histology-anonymizer-subscription"
        self.client = None
        self.consumer = None
        self.running = False
        self.publisher = publisher
        self.handler = HistologyAnonymizationHandler(publisher)
    
    async def start(self):
        """Inicia el consumidor"""
        # Inicializar cliente y consumidor de Pulsar
        self.client = pulsar.Client(self.service_url)
        
        # Crear consumidor
        self.consumer = self.client.subscribe(
            self.topic,
            self.subscription_name,
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        logger.info(f"Consumidor de histología iniciado, suscrito a {self.topic}")
        
        # Iniciar bucle de procesamiento
        self.running = True
        asyncio.create_task(self._process_messages())
    
    async def _process_messages(self):
        """Procesa mensajes recibidos"""
        while self.running:
            try:
                # Recibir mensaje con timeout
                message = self.consumer.receive(timeout_millis=1000)
                
                if message:
                    # Decodificar mensaje
                    data = json.loads(message.data().decode('utf-8'))
                    
                    # Verificar tipo de evento
                    event_type = data.get('type')
                    
                    if event_type == "AnonymizationRequested":
                        # Verificar que sea para histología
                        image_type = data.get('image_type')
                        
                        if image_type == "HISTOLOGY":
                            # Convertir a objeto de evento
                            event = AnonymizationRequested(
                                id=uuid.UUID(data.get('id')) if 'id' in data else uuid.uuid4(),
                                image_id=uuid.UUID(data.get('image_id')) if 'image_id' in data else None,
                                task_id=uuid.UUID(data.get('task_id')) if 'task_id' in data else None,
                                image_type=ImageType.HISTOLOGY,
                                source=data.get('source'),
                                modality=data.get('modality'),
                                region=data.get('region'),
                                file_path=data.get('file_path'),
                                destination_service=data.get('destination_service'),
                                timestamp=datetime.fromisoformat(data.get('timestamp')) if 'timestamp' in data else datetime.now()
                            )
                            
                            # Procesar evento
                            await self.handler.handle(event)
                    
                    # Confirmar mensaje
                    self.consumer.acknowledge(message)
                
            except pulsar.Timeout:
                # Timeout es normal, continuar
                continue
            except Exception as e:
                logger.error(f"Error procesando mensaje: {str(e)}")
                # Pequeña pausa antes de reintentar
                await asyncio.sleep(1)
    
    async def stop(self):
        """Detiene el consumidor"""
        self.running = False
        
        # Esperar un poco para que terminen los procesadores actuales
        await asyncio.sleep(2)
        
        # Cerrar consumidor y cliente
        if self.consumer:
            self.consumer.close()
        
        if self.client:
            self.client.close()
            
        logger.info("Consumidor de histología detenido")