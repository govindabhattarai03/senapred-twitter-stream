import random
import json
from datetime import datetime, timezone
from pymongo import MongoClient
import time

# --------------------------
# MongoDB Setup
# --------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["tweets_chile"]
tweets_collection = db["streaming"]

# --------------------------
# Synthetic Tweet Generator
# --------------------------
CHILE_LOCATIONS = [
    "Santiago", "Valparaíso", "Concepción", "La Serena",
    "Antofagasta", "Temuco", "Puerto Montt", "Iquique"
]

CHILE_KEYWORDS = [
    "terremoto", "sismo", "incendio", "tsunami",
    "emergencia", "alerta", "evacuación", "lluvias"
]

def generate_fake_tweet():
    """Create a synthetic tweet similar to Twitter structure."""
    return {
        "id": random.randint(1000000000000, 9999999999999),
        "text": f"Reporte de {random.choice(CHILE_KEYWORDS)} en {random.choice(CHILE_LOCATIONS)}.",
        "lang": "es",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "author_id": random.randint(1111111, 9999999),
        "geo": {"place_name": random.choice(CHILE_LOCATIONS)},
        "public_metrics": {
            "like_count": random.randint(0, 500),
            "retweet_count": random.randint(0, 200),
            "reply_count": random.randint(0, 50)
        },
        "inserted_at": datetime.now(timezone.utc)  # stored correctly in Mongo
    }

# --------------------------
# Main Loop
# --------------------------
def main():
    print("🚀 Inserting synthetic Chilean tweets into MongoDB…\n")

    while True:
        fake_tweet = generate_fake_tweet()

        # Convert datetime to string only for printing
        printable_tweet = fake_tweet.copy()
        printable_tweet["inserted_at"] = printable_tweet["inserted_at"].isoformat()

        print("Generated synthetic tweet:")
        print(json.dumps(printable_tweet, indent=2, ensure_ascii=False))

        tweets_collection.insert_one(fake_tweet)
        print("✅ Saved to MongoDB\n")

        time.sleep(5)


if __name__ == "__main__":
    main()
