# Servicio de Anonimización

## Descripción general

El Servicio de Anonimización es un microservicio diseñado para anonimizar imágenes médicas antes de que sean procesadas en etapas posteriores de la cadena. Este servicio recibe imágenes que necesitan anonimización, las procesa, y luego notifica al servicio de procesamiento cuando las imágenes están listas para el siguiente paso.

## Arquitectura

El servicio sigue un diseño dirigido por el dominio con un enfoque de arquitectura limpia:

- **Capa de Dominio**: Contiene la lógica de negocio central y las entidades
- **Capa de Aplicación**: Implementa los casos de uso y orquesta los objetos de dominio
- **Capa de Infraestructura**: Proporciona implementaciones para servicios externos y persistencia
- **Capa de API**: Expone la funcionalidad a través de endpoints RESTful

## Características principales

- Recibe notificaciones sobre imágenes que necesitan anonimización
- Crea y rastrea tareas de anonimización en la base de datos
- Realiza la anonimización de imágenes según el tipo (HISTOLOGÍA, RAYOS X, RESONANCIA MAGNÉTICA)
- Actualiza el estado de las tareas y publica eventos al completarse o fallar
- Notifica a los servicios posteriores cuando las imágenes están listas para procesamiento

## Flujo de eventos

1. **Recepción de imagen**: El servicio recibe un evento `ImageReadyForAnonymization` a través de mensajería Pulsar
2. **Creación de tarea**: Los detalles de la imagen se almacenan como una `AnonymizationTask` en la base de datos
3. **Procesamiento de anonimización**: El servicio realiza directamente la anonimización
4. **Completado de tarea**: El estado de la tarea se actualiza con los resultados
5. **Notificación de procesamiento**: Se publica un evento `ImageReadyForProcessing` para notificar a los servicios posteriores

## Endpoints de API

- `POST /api/v1/anonymization/route`: Enruta una imagen para anonimización
- `POST /api/v1/anonymization/tasks/{task_id}/complete`: Marca una tarea como completada
- `POST /api/v1/anonymization/tasks/{task_id}/fail`: Marca una tarea como fallida
- `GET /api/v1/anonymization/health`: Endpoint de verificación de estado

## Tópicos de mensajes

El servicio consume y produce eventos en los siguientes tópicos de Pulsar:

- **Consumo**:
  - `persistent://public/default/image-anonymization`: Recibe eventos `ImageReadyForAnonymization`

- **Producción**:
  - `persistent://public/default/image-processing`: Publica eventos `ImageReadyForProcessing`
  - `persistent://public/default/anonymization-completed`: Publica eventos `AnonymizationCompleted`
  - `persistent://public/default/anonymization-failed`: Publica eventos `AnonymizationFailed`

## Configuración

El servicio puede configurarse mediante variables de entorno o un archivo `.env`:

- `ENVIRONMENT`: Entorno (dev, test, prod)
- `LOG_LEVEL`: Nivel de registro
- `DATABASE_URL`: Cadena de conexión a PostgreSQL
- `PULSAR_SERVICE_URL`: Cadena de conexión a Apache Pulsar
- `API_HOST`: Host para el servidor API
- `API_PORT`: Puerto para el servidor API

## Instalación y despliegue

### Requisitos previos
- Python 3.8+
- PostgreSQL
- Apache Pulsar

### Instalación

1. Clonar el repositorio
2. Instalar dependencias: `pip install -r requirements.txt`
3. Configurar variables de entorno
4. Inicializar la base de datos: `python -m anonymization_service.initialize_db`
5. Iniciar el servicio: `python -m anonymization_service.main`

### Despliegue con Docker

```
docker build -t servicio-anonimizacion .
docker run -p 8001:8001 servicio-anonimizacion
```

## Desarrollo

### Estructura del proyecto

```
anonymization_service/
├── api/                    # Capa de API
├── config/                 # Configuración
├── modules/                # Módulos de funcionalidades
│   └── anonymization/      # Módulo de anonimización
│       ├── application/    # Capa de aplicación
│       ├── domain/         # Capa de dominio
│       └── infrastructure/ # Capa de infraestructura
└── seedwork/               # Componentes compartidos
```

### Ejecución de pruebas

```
pytest tests/
```

## Licencia

Este proyecto está licenciado bajo la Licencia MIT - consulte el archivo LICENSE para más detalles.