import asyncio
from enum import Enum
from typing import List, Dict, Optional
from uuid import uuid4
from heapq import heappush, heappop
from datetime import datetime
import time

class BatchStatus(str, Enum):
    YET_TO_START = "yet_to_start"
    TRIGGERED = "triggered"
    COMPLETED = "completed"

class IngestionProcessor:
    def __init__(self):
        self.jobs: Dict[str, dict] = {}  # Store ingestion jobs
        self.batch_queue = []  # Priority queue for batches
        self.batch_status: Dict[str, BatchStatus] = {}  # Track batch statuses
        self.processing = False
        self.priority_map = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        self._lock = asyncio.Lock()

    async def enqueue_job(self, ingestion_id: str, ids: List[int], priority: str):
        async with self._lock:
            # Split IDs into batches of 3
            batches = [ids[i:i+3] for i in range(0, len(ids), 3)]
            job = {
                "ingestion_id": ingestion_id,
                "batches": [
                    {"batch_id": str(uuid4()), "ids": batch, "status": BatchStatus.YET_TO_START}
                    for batch in batches
                ],
                "priority": priority,
                "created_time": time.time()
            }
            self.jobs[ingestion_id] = job

            # Enqueue batches with priority and creation time
            for batch in job["batches"]:
                self.batch_status[batch["batch_id"]] = BatchStatus.YET_TO_START
                priority_val = self.priority_map[priority]
                heappush(self.batch_queue, (priority_val, job["created_time"], batch["batch_id"]))

            # Start processing if not already running
            if not self.processing:
                self.processing = True
                asyncio.create_task(self.process_batches())

    async def process_batches(self):
        while self.batch_queue:
            async with self._lock:
                if not self.batch_queue:
                    break
                priority, created_time, batch_id = heappop(self.batch_queue)
                self.batch_status[batch_id] = BatchStatus.TRIGGERED

                # Update job status
                for job in self.jobs.values():
                    for batch in job["batches"]:
                        if batch["batch_id"] == batch_id:
                            batch["status"] = BatchStatus.TRIGGERED
                            break

            # Simulate external API call with 5-second delay
            await asyncio.sleep(5)
            async with self._lock:
                self.batch_status[batch_id] = BatchStatus.COMPLETED
                for job in self.jobs.values():
                    for batch in job["batches"]:
                        if batch["batch_id"] == batch_id:
                            batch["status"] = BatchStatus.COMPLETED
                            break
        async with self._lock:
            self.processing = False

    async def get_status(self, ingestion_id: str) -> Optional[dict]:
        async with self._lock:
            if ingestion_id not in self.jobs:
                return None
            job = self.jobs[ingestion_id]
            batches = [
                {
                    "batch_id": batch["batch_id"],
                    "ids": batch["ids"],
                    "status": self.batch_status[batch["batch_id"]]
                }
                for batch in job["batches"]
            ]
            # Determine overall status
            statuses = [self.batch_status[batch["batch_id"]] for batch in job["batches"]]
            if all(s == BatchStatus.YET_TO_START for s in statuses):
                overall_status = BatchStatus.YET_TO_START
            elif all(s == BatchStatus.COMPLETED for s in statuses):
                overall_status = BatchStatus.COMPLETED
            else:
                overall_status = BatchStatus.TRIGGERED
            return {
                "ingestion_id": ingestion_id,
                "status": overall_status,
                "batches": batches
            }