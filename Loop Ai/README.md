# Data Ingestion API System

This is a FastAPI-based system for ingesting and processing data asynchronously, respecting priority and rate limits.

## Features
- **Ingestion API (`POST /ingest`)**: Accepts a list of IDs and a priority (HIGH, MEDIUM, LOW), splits IDs into batches of 3, and enqueues them for processing.
- **Status API (`GET /status/<ingestion_id>`)**: Retrieves the status of an ingestion request, including batch details.
- Asynchronous processing with a priority queue to handle batches based on priority and creation time.
- Rate limiting: Processes 1 batch (up to 3 IDs) every 5 seconds.
- In-memory storage for ingestion and batch statuses.

## Design Choices
- **FastAPI**: Chosen for its asynchronous capabilities and automatic OpenAPI documentation.
- **Priority Queue**: Uses `heapq` to prioritize batches based on priority (HIGH > MEDIUM > LOW) and creation time.
- **In-Memory Storage**: Simplifies persistence for this demo. A database could be used for production.
- **Rate Limiting**: Enforced by a 5-second delay per batch using `asyncio.sleep`.
- **Testing**: Uses `httpx.AsyncClient` for asynchronous testing to handle FastAPI's async nature.

## Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd data-ingestion-api