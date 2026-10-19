"""Generate realistic synthetic emergency tweets.

The X API free tier allows very few requests, so this generator lets the whole
pipeline (storage -> analytics -> API) be developed and demonstrated without it.

Run:  python -m pipeline.ingest_synthetic               (one tweet every few seconds, forever)
      python -m pipeline.ingest_synthetic --count 500   (back-fill 500 tweets over the last 24h)
"""
import argparse
import logging
import random
import time
from datetime import datetime, timedelta, timezone

from pipeline import config
from pipeline.db import ensure_indexes, get_db, upsert_tweets
from pipeline.transform import enrich

log = logging.getLogger("ingest_synthetic")

TEMPLATES = [
    "Reporte de {kw} en {city}.",
    "¡{kw_cap} en {city}! Sigan las indicaciones de SENAPRED.",
    "Fuerte {kw} sentido hace minutos en {city}, ¿alguien más?",
    "Se declara {kw} preventiva en {city} #Chile",
    "Vecinos de {city} reportan {kw} cerca del centro.",
]


def generate_tweet(created_at: datetime | None = None) -> dict:
    kw = random.choice(config.EMERGENCY_KEYWORDS)
    city = random.choice(config.CHILE_LOCATIONS)
    text = random.choice(TEMPLATES).format(kw=kw, kw_cap=kw.capitalize(), city=city)
    return {
        "id": random.randint(10**17, 10**18 - 1),
        "text": text,
        "lang": "es",
        "created_at": created_at or datetime.now(timezone.utc),
        "author_id": random.randint(1_000_000, 9_999_999),
        "geo": {"place_name": city},
        "public_metrics": {
            "like_count": random.randint(0, 500),
            "retweet_count": random.randint(0, 200),
            "reply_count": random.randint(0, 50),
        },
    }


def backfill(db, count: int, hours: int = 24) -> int:
    now = datetime.now(timezone.utc)
    tweets = [
        generate_tweet(now - timedelta(seconds=random.randint(0, hours * 3600)))
        for _ in range(count)
    ]
    return upsert_tweets(db, [enrich(t, source="synthetic") for t in tweets])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, help="back-fill N tweets and exit")
    args = parser.parse_args()

    db = get_db()
    ensure_indexes(db)

    if args.count:
        log.info("Inserted %d synthetic tweets", backfill(db, args.count))
        return

    while True:
        doc = enrich(generate_tweet(), source="synthetic")
        upsert_tweets(db, [doc])
        log.info("%s | %s", doc["location"], doc["text"])
        time.sleep(config.SYNTHETIC_INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
