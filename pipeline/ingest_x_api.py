"""Ingest recent Chilean emergency tweets from the X API v2 into MongoDB.

Run:  python -m pipeline.ingest_x_api          (polls forever)
      python -m pipeline.ingest_x_api --once   (single fetch, useful for testing)
"""
import argparse
import logging
import time

import requests

from pipeline import config
from pipeline.db import ensure_indexes, get_db, upsert_tweets
from pipeline.transform import enrich

log = logging.getLogger("ingest_x_api")


class RateLimited(Exception):
    def __init__(self, wait_seconds: int):
        self.wait_seconds = wait_seconds


def fetch_recent(since_id: str | None = None, max_results: int = 10) -> tuple[list[dict], str | None]:
    """Call the recent-search endpoint. Returns (tweets, newest_id)."""
    params = {
        "query": config.X_QUERY,
        "max_results": max_results,
        "tweet.fields": "created_at,lang,author_id,geo,public_metrics",
    }
    if since_id:
        params["since_id"] = since_id  # only ask for tweets we haven't seen

    response = requests.get(
        config.X_SEARCH_URL,
        headers={"Authorization": f"Bearer {config.X_BEARER_TOKEN}"},
        params=params,
        timeout=30,
    )
    if response.status_code == 429:
        reset = int(response.headers.get("x-rate-limit-reset", time.time() + 900))
        raise RateLimited(max(reset - int(time.time()), 60))
    response.raise_for_status()

    body = response.json()
    return body.get("data", []), body.get("meta", {}).get("newest_id")


def run_once(db, since_id: str | None = None) -> tuple[int, str | None]:
    tweets, newest_id = fetch_recent(since_id)
    new = upsert_tweets(db, [enrich(t, source="x_api") for t in tweets])
    log.info("Fetched %d tweets, %d new", len(tweets), new)
    return new, newest_id or since_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="fetch a single batch and exit")
    args = parser.parse_args()

    if not config.X_BEARER_TOKEN:
        raise SystemExit("X_BEARER_TOKEN is not set. Copy .env.example to .env and add your token.")

    db = get_db()
    ensure_indexes(db)

    since_id = None
    while True:
        try:
            _, since_id = run_once(db, since_id)
        except RateLimited as e:
            log.warning("Rate limited by the X API, sleeping %ds", e.wait_seconds)
            time.sleep(e.wait_seconds)
            continue
        except requests.RequestException as e:
            log.error("Request failed: %s, retrying in 60s", e)
            time.sleep(60)
            continue
        if args.once:
            break
        time.sleep(config.X_POLL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
