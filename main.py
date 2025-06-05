from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, field_validator
from enum import Enum
from typing import List
from uuid import uuid4
import asyncio
from processor import IngestionProcessor

app = FastAPI()
processor = IngestionProcessor()

class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class IngestionRequest(BaseModel):
    ids: List[int]
    priority: Priority

    @field_validator("ids")
    @classmethod
    def validate_ids(cls, ids):
        if not ids:
            raise ValueError("IDs list cannot be empty")
        for id in ids:
            if not (1 <= id <= 10**9 + 7):
                raise ValueError(f"ID {id} is out of range (1 to 10^9+7)")
        return ids

@app.post("/ingest")
async def ingest_data(request: IngestionRequest):
    ingestion_id = str(uuid4())
    await processor.enqueue_job(ingestion_id, request.ids, request.priority)
    return {"ingestion_id": ingestion_id}

@app.get("/status/{ingestion_id}")
async def get_status(ingestion_id: str):
    status = await processor.get_status(ingestion_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Ingestion ID not found")
    return status
