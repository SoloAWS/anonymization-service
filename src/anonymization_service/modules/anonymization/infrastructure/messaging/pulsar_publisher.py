import json
import logging
import pulsar
from typing import Dict, Any
import asyncio

from .....seedwork.domain.events import DomainEvent

logger = logging.getLogger(__name__)

class PulsarPublisher:
    """
    Publicador de mensajes usando Apache Pulsar.
    Se encarga de publicar eventos de dominio en los tópicos configurados.
    """
    
    def __init__(self, service_url: str, topics_mapping: Dict[str, str], token: str, client_config: Dict[str, Any] = None):
        """
        Inicializa el publicador de mensajes
        
        Args:
            service_url: URL del servicio Pulsar
            topics_mapping: Diccionario que mapea tipos de eventos a tópicos
            token: Token de autenticación para Pulsar
            client_config: Configuración adicional para el cliente Pulsar
        """
        self.service_url = service_url
        self.topics_mapping = topics_mapping
        self.token = token
        self.client_config = client_config or {}
        self.client = None
        self.producers = {}
        
    def _initialize(self):
        """Inicializa la conexión a Pulsar si aún no existe"""
        if not self.client:
            try:
                self.client = pulsar.Client(
                    service_url=self.service_url,
                    authentication=pulsar.AuthenticationToken(self.token),
                    operation_timeout_seconds=self.client_config.get('operation_timeout_seconds', 30),
                    io_threads=self.client_config.get('io_threads', 1)
                )
                logger.info("Pulsar client initialized successfully")
            except Exception as e:
                logger.error(f"Error initializing Pulsar client: {str(e)}")
                raise
    
    def _get_topic_for_event(self, event: DomainEvent) -> str:
        """Determina el tópico para un tipo de evento"""
        event_type = event.__class__.__name__
        
        if event_type in self.topics_mapping:
            return self.topics_mapping[event_type]
        
        # Fallback a un tópico por defecto usando el nombre del tipo de evento
        return f"persistent://public/default/anonymization-{event_type.lower()}"
    
    def _get_producer(self, topic: str):
        """Obtiene o crea un productor para un tópico específico"""
        if topic not in self.producers:
            try:
                self.producers[topic] = self.client.create_producer(
                    topic=topic,
                    send_timeout_millis=10000,
                    block_if_queue_full=True,
                    batching_enabled=True,
                    batching_max_messages=100,
                    batching_max_publish_delay_ms=10
                )
                logger.info(f"Created producer for topic: {topic}")
            except Exception as e:
                logger.error(f"Error creating producer for topic {topic}: {str(e)}")
                raise
        
        return self.producers[topic]
    
    async def publish_event(self, event: DomainEvent):
        """
        Publica un evento de dominio en Pulsar
        
        Args:
            event: Evento de dominio a publicar
        """
        try:
            # Asegurarse de que el cliente está inicializado
            self._initialize()
            
            # Determinar el tópico para el evento
            topic = self._get_topic_for_event(event)
            
            # Convertir el evento a un diccionario
            event_dict = event.to_dict()
            
            # Añadir el tipo de evento si no existe
            if 'type' not in event_dict:
                event_dict['type'] = event.__class__.__name__
            
            # Serializar el evento a JSON
            event_json = json.dumps(event_dict)
            
            # Obtener un productor para el tópico
            producer = self._get_producer(topic)
            
            # Enviar el mensaje de forma asíncrona
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, 
                lambda: producer.send(event_json.encode('utf-8'))
            )
            
            logger.info(f"Event {event.__class__.__name__} published to topic {topic}")
        except Exception as e:
            logger.error(f"Error publishing event {event.__class__.__name__}: {str(e)}")
            raise
    
    async def publish_events(self, events: list[DomainEvent]):
        """
        Publica una lista de eventos de dominio en Pulsar
        
        Args:
            events: Lista de eventos de dominio a publicar
        """
        for event in events:
            await self.publish_event(event)
    
    def close(self):
        """Cierra las conexiones con Pulsar"""
        if self.client:
            for topic, producer in self.producers.items():
                try:
                    producer.close()
                except Exception as e:
                    logger.warning(f"Error closing producer for topic {topic}: {str(e)}")
            
            try:
                self.client.close()
                self.client = None
                self.producers = {}
                logger.info("Pulsar client closed successfully")
            except Exception as e:
                logger.warning(f"Error closing Pulsar client: {str(e)}")