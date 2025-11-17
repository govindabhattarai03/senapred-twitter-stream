import json
from datetime import datetime, timezone
from pymongo import MongoClient
from collections import Counter

# -----------------------------
# MongoDB
# -----------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["tweets_chile"]
tweets_collection = db["streaming"]
analytics_collection = db["analytics_results"]


def run_analytics():
    print("📊 Starting analytics...")

    # Load tweets
    print("📥 Fetching tweets from MongoDB...")
    tweets = list(tweets_collection.find({}))

    if not tweets:
        print("⚠ No tweets found in database.")
        return

    print(f"✅ Loaded {len(tweets)} tweets.\n")

    # ----------------------------------------------------
    # ANALYTIC #1 – LOCATION
    # ----------------------------------------------------
    locations = [t["geo"]["place_name"] for t in tweets]
    loc_counter = Counter(locations)

    print("📌 Tweets per location:")
    print(loc_counter, "\n")

    # ----------------------------------------------------
    # ANALYTIC #2 – KEYWORDS
    # ----------------------------------------------------
    keywords = []
    WORDS = ["terremoto", "sismo", "incendio", "tsunami",
             "emergencia", "alerta", "evacuación", "lluvias"]

    for t in tweets:
        for k in WORDS:
            if k in t["text"]:
                keywords.append(k)

    keyword_counter = Counter(keywords)

    print("📌 Tweets per keyword:")
    print(keyword_counter, "\n")

    # ----------------------------------------------------
    # ANALYTIC #3 – HOURLY COUNTS
    # ----------------------------------------------------
    hours = [
        datetime.fromisoformat(t["created_at"]).strftime("%Y-%m-%d %H:00")
        for t in tweets
    ]
    hour_counter = Counter(hours)

    print("📌 Tweets per hour:")
    print(hour_counter, "\n")

    # -----------------------
    # CREATE ANALYTICS OBJECT
    # -----------------------
    analytics_doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "counts_by_location": dict(loc_counter),
        "counts_by_keyword": dict(keyword_counter),
        "counts_by_hour": dict(hour_counter)
    }

    # -----------------------
    # SAVE TO MONGO
    # -----------------------
    inserted_id = analytics_collection.insert_one(analytics_doc).inserted_id
    print("💾 Saved analytics to MongoDB collection 'analytics_results'")

    # Remove ObjectId before JSON export
    analytics_doc["_id"] = str(inserted_id)

    # -----------------------
    # SAVE TO JSON FILE
    # -----------------------
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"analytics_{timestamp}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(analytics_doc, f, indent=2, ensure_ascii=False)

    print(f"📁 Exported analytics to JSON file: {filename}")
    print("🎉 Analytics completed successfully!")


if __name__ == "__main__":
    run_analytics()
