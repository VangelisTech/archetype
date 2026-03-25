# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Memory API routes — ingest, query, list."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from archetype.api.deps import get_container
from archetype.app.container import ServiceContainer

router = APIRouter(prefix="/memory", tags=["memory"])


# ── Request / Response models ─────────────────────────────────────────────────


class IngestRequest(BaseModel):
    session_id: str
    text: str
    source: str = "unknown"


class IngestResponse(BaseModel):
    chunk_id: str
    session_id: str
    is_duplicate: bool
    canonical_id: str = ""
    similarity_score: float = 0.0


class QueryRequest(BaseModel):
    session_id: str
    q: str
    top_k: int = 5


class ChunkResult(BaseModel):
    chunk_id: str
    session_id: str
    content: str
    source: str
    is_duplicate: bool
    similarity_score: float = 0.0
    query_similarity: float = 0.0


# ── Routes ────────────────────────────────────────────────────────────────────


@router.post("/ingest", response_model=IngestResponse)
async def ingest(
    req: IngestRequest,
    container: ServiceContainer = Depends(get_container),
):
    """Ingest a text chunk. Runs MinHash + dedup on the next tick."""
    result = await container.memory_service.ingest(
        session_id=req.session_id,
        text=req.text,
        source=req.source,
    )
    return IngestResponse(
        chunk_id=result.get("chunk_id", ""),
        session_id=result.get("session_id", req.session_id),
        is_duplicate=result.get("is_duplicate", False),
        canonical_id=result.get("canonical_id", ""),
        similarity_score=result.get("similarity_score", 0.0),
    )


@router.post("/query", response_model=list[ChunkResult])
async def query_memory(
    req: QueryRequest,
    container: ServiceContainer = Depends(get_container),
):
    """Retrieve top-K most similar non-duplicate chunks for a query string."""
    results = await container.memory_service.query(
        session_id=req.session_id,
        q=req.q,
        top_k=req.top_k,
    )
    return [
        ChunkResult(
            chunk_id=r.get("chunk_id", ""),
            session_id=r.get("session_id", ""),
            content=r.get("content", ""),
            source=r.get("source", ""),
            is_duplicate=r.get("is_duplicate", False),
            similarity_score=r.get("similarity_score", 0.0),
            query_similarity=r.get("query_similarity", 0.0),
        )
        for r in results
    ]


@router.get("/sessions/{session_id}/chunks", response_model=list[ChunkResult])
async def list_chunks(
    session_id: str,
    container: ServiceContainer = Depends(get_container),
):
    """List all non-duplicate chunks for a session."""
    chunks = await container.memory_service.list_chunks(session_id=session_id)
    return [
        ChunkResult(
            chunk_id=c.get("chunk_id", ""),
            session_id=c.get("session_id", ""),
            content=c.get("content", ""),
            source=c.get("source", ""),
            is_duplicate=c.get("is_duplicate", False),
            similarity_score=c.get("similarity_score", 0.0),
        )
        for c in chunks
    ]
