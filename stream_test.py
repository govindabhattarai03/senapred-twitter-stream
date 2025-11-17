# —– YOUR CONFIGURATION —–
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAIxf5QEAAAAA7m%2B%2FsuP7JhQl5DorEb39rZxVW6g%3D9e8uCVFodgZJ9prZ07NhwSpJcNo5goF739NRbcsceu2Hd5eVZU"
import requests
import time
import json
from datetime import datetime

# ==== CONFIGURATION ====
#BEARER_TOKEN = "X"
SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Accept-Encoding": "gzip"
}

PARAMS = {
    "query": '(Santiago OR "Región Metropolitana" OR Chile) lang:es -is:retweet',
    "max_results": 100,
    "tweet.fields": "created_at,lang,author_id,geo,public_metrics,text",
    "expansions": "author_id",
    "user.fields": "id,location,username"
}

def fetch_tweets():
    """Fetch recent tweets based on the defined query."""
    response = requests.get(SEARCH_URL, headers=HEADERS, params=PARAMS, timeout=30)
    if response.status_code != 200:
        raise Exception(f"Error {response.status_code}: {response.text}")
    return response.json()

def process_tweets(data):
    """Print and save tweets to a file."""
    tweets = data.get("data", [])
    if not tweets:
        print("No recent tweets found.")
        return

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"tweets_{timestamp}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(tweets, f, ensure_ascii=False, indent=2)

    print(f"{len(tweets)} tweets saved to {filename}")
    print("-" * 80)

    for t in tweets:
        print(f"{t['created_at']} | Author ID: {t['author_id']}")
        print(t["text"])
        print("-" * 80)

def main():
    """Fetch tweets every 15 minutes."""
    INTERVAL = 15 * 60  # 15 minutes = 900 seconds
    backoff = 60  # seconds for retry

    while True:
        try:
            print(f"\nFetching tweets... {datetime.now().strftime('%H:%M:%S')}")
            data = fetch_tweets()
            process_tweets(data)
            print(f"Waiting {INTERVAL / 60:.0f} minutes before next fetch...\n")
            time.sleep(INTERVAL)
        except Exception as e:
            print("Error:", e)
            print(f"Retrying in {backoff} seconds...\n")
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)  # max 5 minutes
            continue

if __name__ == "__main__":
    main()
