import logging
import json
import uuid
from typing import Dict, Any, Callable, Awaitable
import pulsar
import asyncio
from datetime import datetime

from .....seedwork.domain.events import DomainEvent
from ...domain.events import (
    ImageReadyForAnonymization,
    AnonymizationCompleted,
    AnonymizationFailed,
)
from ...domain.value_objects import ImageType

logger = logging.getLogger(__name__)


class PulsarConsumer:
    """
    Consumidor de mensajes usando Apache Pulsar.
    Se encarga de suscribirse a los tópicos configurados y procesar los mensajes.
    """

    def __init__(
        self,
        service_url: str,
        topics_mapping: Dict[str, str],
        token: str,
        client_config: Dict[str, Any] = None,
    ):
        """
        Inicializa el consumidor de mensajes

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
        self.consumers = {}
        self.event_handlers = {}
        self.running = False
        self.command_handlers = {}

    def _initialize(self):
        """Inicializa la conexión a Pulsar si aún no existe"""
        if not self.client:
            try:
                # self.client = pulsar.Client(
                #     "pulsar+ssl://pc-286ed2b3.gcp-shared-usce1.g.snio.cloud:6651",
                #     authentication=pulsar.AuthenticationToken(
                #         "eyJhbGciOiJSUzI1NiIsImtpZCI6IjgzODJkMDdiLWJjNmQtNTJhZi04ZDY2LTNiZWRhNDJmMjNjMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsidXJuOnNuOnB1bHNhcjpzZXJ2ZXJsZXNzOnBpLTdhOWU3MzNiIl0sImV4cCI6MTc0MzU0MjUwOCwiaHR0cHM6Ly9zdHJlYW1uYXRpdmUuaW8vc2NvcGUiOlsiYWRtaW4iLCJhY2Nlc3MiXSwiaHR0cHM6Ly9zdHJlYW1uYXRpdmUuaW8vdXNlcm5hbWUiOiJhZG1pbnNlcnZpY2VhY2NvdW50QG8tbGd3eXAuYXV0aC5zdHJlYW1uYXRpdmUuY2xvdWQiLCJpYXQiOjE3NDA5NTA1MTIsImlzcyI6Imh0dHBzOi8vcGMtMjg2ZWQyYjMuZ2NwLXNoYXJlZC11c2NlMS5nLnNuaW8uY2xvdWQvYXBpa2V5cy8iLCJqdGkiOiIwNmQxMmRiMzU0MGY0ZWE3OTc4YjJlM2NkNzU4OTQ3MCIsInBlcm1pc3Npb25zIjpbXSwic3ViIjoiRHV0bnVsN1NiTlpvbUFtd21UR0VCQ0JEMU5uaDZpc2hAY2xpZW50cyJ9.2SvuidJzGeZ99fPS4oCbLrK3ESH9JKi25RoAIJSUeDpI9_EJHxN-SCBwaknZHBAlM4ae9CEndrq8cwrB3f4M85Cvxmc6-kuit7RG48cnYC8ajzR7OFkJkELico_NtIBECQXVQO0br9bRly7aQjNSpQitYNif0WL8G1oS_kVKI-suXXPrvrO1zenCvriaXIXqYrRPvxF5v-SVMksXCdxdkwhGmUVkkG76GZ7Tulwu7UUlb1xQBwij-iT1EOzOgxInKlhvzDiZrPx0IYbHvxZc8lICXEhfowxyRyxAkAJ6KZnCyyQ4cN1WmDR5UdwN4z5j3XV9OIg3V3g8739idwo-SQ"
                #     ),
                # )
                self.client = pulsar.Client(
                    service_url=self.service_url,
                    authentication=pulsar.AuthenticationToken(self.token),
                )

                logger.info("Pulsar client initialized successfully")
            except Exception as e:
                logger.error(f"Error initializing Pulsar client: {str(e)}")
                raise

    def register_event_handler(
        self, event_type: str, handler_func: Callable[[Dict[str, Any]], Awaitable[None]]
    ):
        """
        Registra un manejador para un tipo de evento específico

        Args:
            event_type: Tipo de evento a manejar
            handler_func: Función asíncrona que maneja el evento
        """
        self.event_handlers[event_type] = handler_func
        logger.info(f"Registered handler for event type: {event_type}")

    def _create_consumer(self, topic: str, subscription_name: str):
        """
        Crea un consumidor para un tópico específico

        Args:
            topic: Nombre del tópico
            subscription_name: Nombre de la suscripción
        """
        try:
            consumer = self.client.subscribe(
                topic=topic,
                subscription_name=subscription_name,
                initial_position=pulsar.InitialPosition.Earliest,
            )
            logger.info(
                f"Created consumer for topic: {topic}, subscription: {subscription_name}"
            )
            return consumer
        except Exception as e:
            logger.error(f"Error creating consumer for topic {topic}: {str(e)}")
            raise

    async def _process_message(self, consumer, message):
        """
        Procesa un mensaje recibido

        Args:
            consumer: Consumidor que recibió el mensaje
            message: Mensaje recibido
        """
        try:
            # Decodificar el mensaje
            data = json.loads(message.data().decode("utf-8"))

            # Extraer el tipo de mensaje (evento o comando)
            message_type = data.get("type")

            if not message_type:
                logger.warning(f"Received message without type: {data}")
                consumer.acknowledge(message)
                return

            logger.info(f"Received message of type {message_type}: {data}")

            # Verificar si es un comando
            if message_type in self.command_handlers:
                logger.info(f"Processing command: {message_type}")
                handler = self.command_handlers[message_type]
                
                # Obtener datos del comando
                command_data = data.get("data", {})
                correlation_id = data.get("correlation_id")
                
                # Procesar comando
                await handler(command_data, correlation_id)
                logger.info(f"Command {message_type} processed successfully")
            # Si no es comando, verificar si es un evento conocido
            elif message_type in self.event_handlers:
                # Convertir el mensaje en un evento de dominio
                event = self._create_event_from_data(message_type, data)

                # Procesar el evento
                if event:
                    handler = self.event_handlers[message_type]
                    await handler(event)
                    logger.info(f"Event {message_type} processed successfully")
                else:
                    logger.warning(f"Failed to create event from data: {data}")
            else:
                logger.warning(f"No handler registered for message type: {message_type}")

            # Confirmar el mensaje
            consumer.acknowledge(message)
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            # Negar el mensaje para que pueda ser reprocesado
            consumer.negative_acknowledge(message)

    def _create_event_from_data(
        self, event_type: str, data: Dict[str, Any]
    ) -> DomainEvent:
        """
        Crea un evento de dominio a partir de los datos recibidos

        Args:
            event_type: Tipo de evento
            data: Datos del evento

        Returns:
            Un evento de dominio correspondiente al tipo
        """
        try:
            if event_type == "ImageReadyForAnonymization":
                # Determinar el tipo de imagen
                image_type = ImageType.UNKNOWN
                modality = data.get("modality", "").upper()

                if modality == "HISTOLOGY" or "HIST" in modality:
                    image_type = ImageType.HISTOLOGY
                elif modality == "XRAY" or "RAY" in modality:
                    image_type = ImageType.XRAY
                elif modality == "MRI" or "MAGNETIC" in modality:
                    image_type = ImageType.MRI

                return ImageReadyForAnonymization(
                    id=uuid.UUID(data.get("id")) if "id" in data else uuid.uuid4(),
                    image_id=(
                        uuid.UUID(data.get("image_id")) if "image_id" in data else None
                    ),
                    task_id=(
                        uuid.UUID(data.get("task_id")) if "task_id" in data else None
                    ),
                    source=data.get("source"),
                    modality=data.get("modality"),
                    region=data.get("region"),
                    file_path=data.get("file_path"),
                    image_type=image_type,
                    timestamp=(
                        datetime.fromisoformat(data.get("timestamp"))
                        if "timestamp" in data
                        else datetime.now()
                    ),
                )
            else:
                logger.warning(f"Tipo de evento desconocido: {event_type}")
                return None
        except Exception as e:
            logger.error(f"Error al crear evento a partir de los datos: {str(e)}")
            return None

    async def start_listening(self):
        """Inicia la escucha de mensajes en todos los tópicos configurados"""
        self._initialize()
        self.running = True

        # Solo nos suscribimos al tópico de ImageReadyForAnonymization
        for event_type, topic in self.topics_mapping.items():
            # Solo procesar los eventos que necesitamos
            if event_type == "ImageReadyForAnonymization":
                logger.info(f"Configurando consumidor para el tópico: {topic}")
                subscription_name = f"anonymization-service-{event_type.lower()}"
                consumer = self._create_consumer(topic, subscription_name)
                self.consumers[topic] = consumer

                # Iniciar tarea asíncrona para procesar mensajes
                logger.info(f"Creando tarea asincrónica para el tópico: {topic}")
                asyncio.create_task(self._listen_for_messages(consumer))

        logger.info("Iniciada la escucha de mensajes en los tópicos configurados")

    async def _listen_for_messages(self, consumer):
        """
        Escucha mensajes de un consumidor específico

        Args:
            consumer: Consumidor de Pulsar
        """
        topic = None
        for t, c in self.consumers.items():
            if c == consumer:
                topic = t
                break
        logger.info(f"Starting to listen for messages on consumer for topic: {topic}")

        while self.running:
            try:
                # Use run_in_executor to make the blocking receive call non-blocking
                loop = asyncio.get_event_loop()
                message = await loop.run_in_executor(
                    None, lambda: consumer.receive(timeout_millis=1000)
                )

                if message:
                    logger.info(f"Received message: {message}")
                    await self._process_message(consumer, message)

            except pulsar.Timeout:
                # Timeout is normal, continue
                continue
            except Exception as e:
                logger.error(
                    f"Error receiving message: {str(e)}", exc_info=True
                )  # Add exc_info for more details
                await asyncio.sleep(1)  # Small pause before retrying

    async def stop(self):
        """Detiene la escucha de mensajes y libera recursos"""
        self.running = False

        # Esperar un poco para que los procesadores actuales terminen
        await asyncio.sleep(2)

        # Cerrar consumidores
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()
            except Exception as e:
                logger.warning(f"Error closing consumer for topic {topic}: {str(e)}")

        # Cerrar cliente
        if self.client:
            try:
                self.client.close()
                self.client = None
            except Exception as e:
                logger.warning(f"Error closing Pulsar client: {str(e)}")

        self.consumers = {}
        logger.info("Stopped listening for messages and released all resources")
        
        
    def register_command_handler(
        self, command_type: str, handler_func: Callable[[Dict[str, Any]], Awaitable[None]]
    ):
        """
        Registra un manejador para un tipo de comando específico

        Args:
            command_type: Tipo de comando a manejar
            handler_func: Función asíncrona que maneja el comando
        """
        self.command_handlers[command_type] = handler_func
        logger.info(f"Registered handler for command type: {command_type}")
