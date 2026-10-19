"""Compute emergency analytics with MongoDB aggregation pipelines.

The counting runs inside the database, so it scales to millions of tweets
without loading them into Python memory.

Run:  python -m pipeline.analytics
"""
import json
import logging
from datetime import datetime, timedelta, timezone

from pipeline.db import get_db

log = logging.getLogger("analytics")


def _count_by(db, group_key, match: dict, pre_stages: list | None = None) -> dict:
    pipeline = [{"$match": match}, *(pre_stages or [])]
    pipeline += [
        {"$group": {"_id": group_key, "count": {"$sum": 1}}},
        {"$match": {"_id": {"$ne": None}}},
        {"$sort": {"count": -1}},
    ]
    return {row["_id"]: row["count"] for row in db.tweets.aggregate(pipeline)}


def compute_analytics(db, window_hours: int = 24) -> dict:
    """Aggregate tweets from the last `window_hours` and store the result."""
    now = datetime.now(timezone.utc)
    match = {"created_at": {"$gte": now - timedelta(hours=window_hours)}}

    by_hour = _count_by(
        db, {"$dateToString": {"format": "%Y-%m-%d %H:00", "date": "$created_at"}}, match
    )

    result = {
        "generated_at": now,
        "window_hours": window_hours,
        "total_tweets": db.tweets.count_documents(match),
        "counts_by_location": _count_by(db, "$location", match),
        "counts_by_keyword": _count_by(db, "$keywords", match, [{"$unwind": "$keywords"}]),
        "counts_by_hour": dict(sorted(by_hour.items())),
        "counts_by_source": _count_by(db, "$source", match),
    }
    db.analytics_results.insert_one(dict(result))
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    summary = compute_analytics(get_db())
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
