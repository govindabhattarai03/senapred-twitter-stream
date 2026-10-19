"""End-to-end tests against an in-memory MongoDB (mongomock): ingest -> analytics -> API."""
import mongomock
import pytest
from fastapi.testclient import TestClient

import pipeline.db as dbmod
from pipeline.analytics import compute_analytics
from pipeline.db import ensure_indexes, upsert_tweets
from pipeline.ingest_synthetic import backfill
from pipeline.transform import enrich


@pytest.fixture
def db(monkeypatch):
    client = mongomock.MongoClient(tz_aware=True)
    monkeypatch.setattr(dbmod, "_client", client)
    database = client["senapred_test"]
    monkeypatch.setattr(dbmod.config, "DB_NAME", "senapred_test")
    ensure_indexes(database)
    return database


def test_upsert_is_idempotent(db):
    doc = enrich({"id": 1, "text": "sismo en Temuco"}, source="synthetic")
    assert upsert_tweets(db, [doc]) == 1
    assert upsert_tweets(db, [doc]) == 0  # same tweet again: no duplicate
    assert db.tweets.count_documents({}) == 1


def test_analytics_counts_match_data(db):
    backfill(db, count=200)
    result = compute_analytics(db)
    assert result["total_tweets"] == 200
    assert sum(result["counts_by_location"].values()) == 200
    assert sum(result["counts_by_hour"].values()) == 200
    assert result["counts_by_source"] == {"synthetic": 200}


def test_api_endpoints(db):
    from api.main import app

    backfill(db, count=50)
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}
        assert client.get("/analytics/latest").status_code == 404
        assert client.post("/analytics/run").json()["total_tweets"] == 50
        assert sum(client.get("/analytics/keywords").json().values()) >= 50
        assert len(client.get("/tweets/latest?limit=5").json()) == 5
        assert client.get("/analytics/run").status_code == 405  # state changes are POST-only
