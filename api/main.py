"""FastAPI service exposing tweets and emergency analytics to the dashboard.

Run:  uvicorn api.main:app --reload     then open http://localhost:8000/docs
"""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from pipeline.analytics import compute_analytics
from pipeline.db import ensure_indexes, get_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_indexes(get_db())
    yield


app = FastAPI(
    title="SENAPRED Emergency Tweet API",
    version="2.0.0",
    description="Real-time monitoring of emergency-related tweets in Chile.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def _latest_analytics() -> dict:
    doc = get_db().analytics_results.find_one({}, {"_id": 0}, sort=[("generated_at", -1)])
    if not doc:
        raise HTTPException(status_code=404, detail="No analytics yet. Call POST /analytics/run first.")
    return doc


@app.get("/health")
def health():
    get_db().command("ping")
    return {"status": "ok"}


@app.get("/tweets/latest")
def latest_tweets(limit: int = Query(20, ge=1, le=100), keyword: str | None = None):
    query = {"keywords": keyword} if keyword else {}
    cursor = get_db().tweets.find(query, {"_id": 0}).sort("created_at", -1).limit(limit)
    return list(cursor)


@app.get("/analytics/latest")
def analytics_latest():
    return _latest_analytics()


@app.get("/analytics/locations")
def analytics_locations():
    return _latest_analytics()["counts_by_location"]


@app.get("/analytics/keywords")
def analytics_keywords():
    return _latest_analytics()["counts_by_keyword"]


@app.get("/analytics/hours")
def analytics_hours():
    return _latest_analytics()["counts_by_hour"]


@app.post("/analytics/run")
def analytics_run(window_hours: int = Query(24, ge=1, le=24 * 30)):
    """Recompute analytics now. POST, because it changes stored data."""
    return compute_analytics(get_db(), window_hours)
