"""MongoDB access: one shared client, indexes and idempotent writes."""
from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne

from pipeline import config

_client = None


def get_db():
    global _client
    if _client is None:
        _client = MongoClient(config.MONGO_URI, tz_aware=True)
    return _client[config.DB_NAME]


def ensure_indexes(db) -> None:
    # A unique index on tweet_id makes re-ingesting the same tweet impossible.
    db.tweets.create_index([("tweet_id", ASCENDING)], unique=True)
    db.tweets.create_index([("created_at", DESCENDING)])
    db.analytics_results.create_index([("generated_at", DESCENDING)])


def upsert_tweets(db, docs: list[dict]) -> int:
    """Insert new tweets and update existing ones in a single round trip.

    Returns the number of tweets that were new.
    """
    if not docs:
        return 0
    ops = [UpdateOne({"tweet_id": d["tweet_id"]}, {"$set": d}, upsert=True) for d in docs]
    result = db.tweets.bulk_write(ops, ordered=False)
    return result.upserted_count
