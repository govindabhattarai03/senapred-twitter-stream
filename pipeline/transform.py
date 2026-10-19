"""Pure transformation functions: raw tweet -> clean, enriched document.

Kept free of any database or network code so they are easy to unit-test.
"""
import re
import unicodedata
from datetime import datetime, timezone

from pipeline.config import CHILE_LOCATIONS, EMERGENCY_KEYWORDS


def normalize(text: str) -> str:
    """Lower-case and strip accents, so 'Evacuación' matches 'evacuacion'."""
    decomposed = unicodedata.normalize("NFKD", text or "")
    return "".join(c for c in decomposed if not unicodedata.combining(c)).lower()


def find_keywords(text: str, keywords=EMERGENCY_KEYWORDS) -> list[str]:
    """Return the emergency keywords mentioned in the text (sorted, no duplicates).

    A keyword matches at the start of a word, so 'sismo' also matches 'sismos'.
    """
    clean = normalize(text)
    return sorted(k for k in keywords if re.search(r"\b" + re.escape(normalize(k)), clean))


def find_location(text: str, geo: dict | None = None, locations=CHILE_LOCATIONS) -> str | None:
    """Use the tweet's place name when present, otherwise look for a known city in the text."""
    if geo and geo.get("place_name"):
        return geo["place_name"]
    clean = normalize(text)
    for city in locations:
        if re.search(r"\b" + re.escape(normalize(city)) + r"\b", clean):
            return city
    return None


def parse_created_at(value) -> datetime:
    """Accept a datetime or an ISO-8601 string (the X API returns '...Z') and return UTC."""
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def enrich(tweet: dict, source: str) -> dict:
    """Turn a raw tweet (from the X API or the generator) into the stored document."""
    text = tweet.get("text", "")
    return {
        "tweet_id": str(tweet["id"]),
        "text": text,
        "lang": tweet.get("lang"),
        "author_id": str(tweet["author_id"]) if tweet.get("author_id") else None,
        "created_at": parse_created_at(tweet.get("created_at") or datetime.now(timezone.utc)),
        "location": find_location(text, tweet.get("geo")),
        "keywords": find_keywords(text),
        "public_metrics": tweet.get("public_metrics", {}),
        "source": source,
        "ingested_at": datetime.now(timezone.utc),
    }
