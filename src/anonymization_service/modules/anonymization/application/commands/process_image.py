from dataclasses import dataclass
from .....seedwork.application.commands import Command, CommandHandler
from ...domain.entities import ProcessingTask
from ...domain.repositories import ProcessingRepository
from ...domain.value_objects import (
    ProcessingMetadata,
    ImageType,
    ProcessingStatus,
    ProcessingResult,
)
from ..events.event_handlers import PulsarEventHandler
import os


@dataclass
class ProcessImageCommand(Command):
    image_type: str
    region: str
    raw_data: bytes
    priority: int = 0


class ProcessImageHandler(CommandHandler):
    def __init__(
        self, repository: ProcessingRepository, event_handler: PulsarEventHandler
    ):
        self.repository = repository
        self.event_handler = event_handler

    async def handle(self, command: ProcessImageCommand):
        # Create metadata and task
        metadata = ProcessingMetadata(
            image_type=ImageType[command.image_type],
            region=command.region,
            priority=command.priority,
        )

        task = ProcessingTask(metadata=metadata, raw_data=command.raw_data)

        # Emit ProcessingStarted event
        started_event = task.start_processing()
        await self.event_handler.handle(started_event)

        try:
            # Here would go the actual processing logic
            # For now, we'll just simulate success
            result = ProcessingResult(
                status=ProcessingStatus.COMPLETED,
                message="Processing completed successfully",
            )

            # Update task with result
            completed_event = task.complete_processing(result)

            # Save to database
            await self.repository.save(task)

            # Emit ProcessingCompleted event
            await self.event_handler.handle(completed_event)

            return task.id

        except Exception as e:
            # Handle failure
            failed_event = task.fail_processing(str(e))
            await self.repository.save(task)
            await self.event_handler.handle(failed_event)
            raise
