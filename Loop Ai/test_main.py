import pytest
import httpx
import asyncio
from fastapi.testclient import TestClient
from main import app
from time import time

client = TestClient(app)

@pytest.mark.asyncio
async def test_ingest_valid_input():
    payload = {"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"}
    response = client.post("/ingest", json=payload)
    assert response.status_code == 200
    assert "ingestion_id" in response.json()

@pytest.mark.asyncio
async def test_ingest_invalid_ids():
    payload = {"ids": [0, 2, 3], "priority": "MEDIUM"}  # ID 0 is invalid
    response = client.post("/ingest", json=payload)
    assert response.status_code == 422

    payload = {"ids": [1, 10**9 + 8], "priority": "MEDIUM"}  # ID too large
    response = client.post("/ingest", json=payload)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_ingest_empty_ids():
    payload = {"ids": [], "priority": "MEDIUM"}
    response = client.post("/ingest", json=payload)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_ingest_invalid_priority():
    payload = {"ids": [1, 2, 3], "priority": "INVALID"}
    response = client.post("/ingest", json=payload)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_status_not_found():
    response = client.get("/status/nonexistent")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_priority_and_rate_limit():
    # Request 1: MEDIUM priority at T0
    payload1 = {"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"}
    response1 = client.post("/ingest", json=payload1)
    ingestion_id1 = response1.json()["ingestion_id"]

    # Request 2: HIGH priority at T4
    await asyncio.sleep(4)
    payload2 = {"ids": [6, 7, 8, 9], "priority": "HIGH"}
    response2 = client.post("/ingest", json=payload2)
    ingestion_id2 = response2.json()["ingestion_id"]

    # Wait for processing to start
    await asyncio.sleep(1)
    response = client.get(f"/status/{ingestion_id1}")
    assert response.status_code == 200
    status = response.json()
    assert status["status"] == "triggered"
    assert len(status["batches"]) == 2
    assert status["batches"][0]["ids"] == [1, 2, 3]
    assert status["batches"][0]["status"] in ["triggered", "completed"]

    # Wait for first batch to complete and second to start
    await asyncio.sleep(5)
    response = client.get(f"/status/{ingestion_id2}")
    assert response.status_code == 200
    status = response.json()
    assert status["status"] == "triggered"
    assert len(status["batches"]) == 2
    assert status["batches"][0]["ids"] == [6, 7, 8]
    assert status["batches"][0]["status"] in ["triggered", "completed"]

    # Wait for all batches to complete
    await asyncio.sleep(10)
    response = client.get(f"/status/{ingestion_id1}")
    assert response.status_code == 200
    status = response.json()
    assert status["status"] == "completed"
    assert all(batch["status"] == "completed" for batch in status["batches"])

    response = client.get(f"/status/{ingestion_id2}")
    assert response.status_code == 200
    status = response.json()
    assert status["status"] == "completed"
    assert all(batch["status"] == "completed" for batch in status["batches"])

@pytest.mark.asyncio
async def test_rate_limit():
    start_time = time()
    payload = {"ids": [1, 2, 3, 4, 5, 6, 7], "priority": "MEDIUM"}
    response = client.post("/ingest", json=payload)
    ingestion_id = response.json()["ingestion_id"]

    # Wait for first batch to complete
    await asyncio.sleep(6)
    response = client.get(f"/status/{ingestion_id}")
    status = response.json()
    assert status["batches"][0]["status"] == "completed"
    assert status["batches"][1]["status"] in ["yet_to_start", "triggered"]

    # Ensure second batch doesn't complete before 10 seconds
    elapsed = time() - start_time
    if elapsed < 10:
        await asyncio.sleep(10 - elapsed)
    response = client.get(f"/status/{ingestion_id}")
    status = response.json()
    assert status["batches"][1]["status"] == "completed"