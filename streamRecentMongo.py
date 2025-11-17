import requests
import json
import time
from pymongo import MongoClient
from datetime import datetime

# --------------------------
# CONFIGURATION
# --------------------------
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAIxf5QEAAAAA7m%2B%2FsuP7JhQl5DorEb39rZxVW6g%3D9e8uCVFodgZJ9prZ07NhwSpJcNo5goF739NRbcsceu2Hd5eVZU"
MONGO_URI = "mongodb://localhost:27017"
QUERY = "python lang:en"
MAX_RESULTS = 10

# --------------------------
# DATABASE CONNECTION
# --------------------------
client = MongoClient(MONGO_URI)
db = client["tweets_recent"]
tweets_collection = db["recent_data"]

# --------------------------
# FETCH RECENT TWEETS
# --------------------------
def fetch_recent_tweets():
    url = "https://api.x.com/2/tweets/search/recent"
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    params = {
        "query": QUERY,
        "max_results": MAX_RESULTS,
        "tweet.fields": "created_at,lang,author_id"
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print(f"Error: Connection error: {response.status_code} – {response.text}")
        return []

    data = response.json()
    return data.get("data", [])

# --------------------------
# SAVE TO MONGODB
# --------------------------
def save_to_mongo(tweets):
    for tweet in tweets:
        tweet_doc = {
            "id": tweet["id"],
            "text": tweet["text"],
            "author_id": tweet["author_id"],
            "created_at": tweet["created_at"],
            "lang": tweet["lang"],
            "inserted_at": datetime.utcnow()
        }

        # Avoid duplicates (update if exists)
        tweets_collection.update_one(
            {"id": tweet_doc["id"]},
            {"$set": tweet_doc},
            upsert=True
        )

# --------------------------
# MAIN LOOP
# --------------------------
if __name__ == "__main__":
    backoff = 1
    while True:
        print("Fetching recent tweets …")
        tweets = fetch_recent_tweets()

        if tweets:
            print(f"Fetched {len(tweets)} tweets. Saving to MongoDB …")
            save_to_mongo(tweets)
            backoff = 1  # reset backoff
        else:
            print("No tweets fetched.")

        print("Waiting 60 seconds before next fetch …\n")
        time.sleep(60)
