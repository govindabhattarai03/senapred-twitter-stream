import requests
from pymongo import MongoClient
from datetime import datetime, timezone
import time
import json

# -------------------------------
# CONFIGURATION
# -------------------------------
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAIxf5QEAAAAA7m%2B%2FsuP7JhQl5DorEb39rZxVW6g%3D9e8uCVFodgZJ9prZ07NhwSpJcNo5goF739NRbcsceu2Hd5eVZU"
MONGO_URI = "mongodb://localhost:27017/"
#QUERRY = "Chile lang:es"
QUERY = '(Chile OR Santiago OR Valparaíso OR Concepción OR Boric OR "La Moneda" OR #Chile) lang:es'

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client["tweets_chile"]
tweets_collection = db["recent_data"]

# -------------------------------
# FUNCTION TO FETCH RECENT TWEETS
# -------------------------------
def fetch_recent_tweets():
    url = "https://api.x.com/2/tweets/search/recent"
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    params = {
        "query": QUERY,
        "max_results": 10,
        "tweet.fields": "created_at,lang,author_id,geo,public_metrics"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        raise Exception(f"Connection error: {response.status_code} – {response.text}")

    data = response.json()
    return data.get("data", [])

# -------------------------------
# MAIN LOOP
# -------------------------------
def main():
    while True:
        try:
            print("Fetching recent Chilean tweets …")
            tweets = fetch_recent_tweets()
            print(f"Fetched {len(tweets)} tweets. Saving to MongoDB …")

            for tweet in tweets:
                tweet_record = {
                    "id": tweet["id"],
                    "text": tweet["text"],
                    "lang": tweet.get("lang"),
                    "author_id": tweet.get("author_id"),
                    "created_at": tweet.get("created_at"),
                    "geo": tweet.get("geo"),
                    "public_metrics": tweet.get("public_metrics"),
                    "inserted_at": datetime.now(timezone.utc)
                }
                tweets_collection.insert_one(tweet_record)

            print("Tweets saved to MongoDB successfully.\n")
            print("Waiting 60 seconds before next fetch …")
            time.sleep(60)

        except Exception as e:
            print("Error:", e)
            print("Retrying in 30 seconds …")
            time.sleep(30)

# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    main()
