from typing import List, Dict

from uuid import UUID
import pyarrow as pa

from .command import Command
from .base import BaseCommandBroker


class SyncCommandBroker(BaseCommandBroker):
    """
    A Simple Synchronous Command queue for commands based interactivity with the world.
    """
    def __init__(self, max_dequeue: int = 50_000):

        self._queue: Dict[str, List[Command]] = []
        self._parquet_buffer: List[pa.RecordBatch] = []
        
        # Configuration
        self._max_dequeue = max_dequeue
        self._flush_size_bytes = 512 * 1024 * 1024  # 512 MB

    
    def enqueue(self, world_id: str, cmd: Command) -> None:
        """
        Enqueue a command into the queue.
        """
        if world_id not in self._queue:
            self._queue[world_id] = []
        self._queue[world_id].append(cmd)

    def enqueue_bulk(self,  world_id: str, cmds: List["Command"]) -> None:
        """
        Enqueue multiple commands into the queue.
        """
        assert world_id is not None, "World ID must be provided for enqueueing commands."
        assert cmds is not None, "Commands list must not be empty."


        if world_id not in self._queue:
            self._queue[world_id] = []
        self._queue[world_id].extend(cmds)
    
    def dequeue_due(self,  world_id: str, *, tick: int, limit: int = 1_000) -> List[Command]: 
        """
        Dequeue commands that are due for processing.
        
        Args:
            tick: The current tick to check for due commands.
            limit: Maximum number of commands to dequeue.
        
        Returns:
            A list of commands that are due for processing.
        """
        due_commands = [cmd for cmd in sorted(self._queue[world_id]) if cmd.tick <= tick]
        # Acknowledge the commands being returned
        self.ack([cmd.id for cmd in due_commands])
        return due_commands[:limit]
    
    def ack(self,  world_id: str, cmd_ids: List[UUID]) -> None:
        """
        Acknowledge that commands with the given IDs have been processed.
        
        Args:
            cmd_ids: List of command IDs to acknowledge.
        """
        self._queue[world_id] = [cmd for cmd in self._queue[world_id] if cmd.id not in cmd_ids]

    async def _flush_parquet(self):
        if not self._parquet_buffer:
            return

        buffer_to_flush = None
        async with self._lock:
            if self._parquet_buffer:
                buffer_to_flush = self._parquet_buffer
                self._parquet_buffer = []

        if not buffer_to_flush:
            return

        log_dir = "command_log"
        os.makedirs(log_dir, exist_ok=True)

        tbl = pa.Table.from_batches(buffer_to_flush)
        pq.write_table(
            tbl,
            os.path.join(log_dir, f"{date.today():%Y-%m-%d}.parquet"),
            schema=Command.arrow_schema(),
            compression="zstd",
        )

    