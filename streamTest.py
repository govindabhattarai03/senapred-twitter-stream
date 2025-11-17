import requests
import time
import json
import gzip
import io

# —– YOUR CONFIGURATION —–
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAIxf5QEAAAAA7m%2B%2FsuP7JhQl5DorEb39rZxVW6g%3D9e8uCVFodgZJ9prZ07NhwSpJcNo5goF739NRbcsceu2Hd5eVZU"

# Use the "recent search" endpoint instead of streaming
STREAM_URL = "https://api.x.com/2/tweets/search/recent"

# Authorization headers
headers = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Accept-Encoding": "gzip"
}

def connect_to_stream(url, headers, timeout=90):
    """Make a one-time request to the recent search endpoint."""
    params = {
        "query": "earthquake OR flooding lang:es",  # you can change this later (e.g., "Chile lang:es")
        "max_results": 10,
        "tweet.fields": "created_at,lang,author_id"
    }
    response = requests.get(url, headers=headers, params=params, timeout=timeout)
    if response.status_code != 200:
        raise Exception(f"Connection error: {response.status_code} – {response.text}")
    return response

def consume_stream(response):
    """Print the tweets returned by the recent search."""
    try:
        data = response.json()
    except json.JSONDecodeError:
        print("Error decoding JSON.")
        return

    if "data" not in data:
        print("No tweets found or error in response.")
        return

    for tweet in data["data"]:
        print(json.dumps(tweet, indent=2, ensure_ascii=False))
        print("-" * 80)

def main():
    """Repeatedly fetch recent tweets every minute (simulates streaming)."""
    backoff = 1
    while True:
        try:
            print("Fetching recent tweets …")
            resp = connect_to_stream(STREAM_URL, headers)
            print("Connected. Displaying results …")
            consume_stream(resp)
        except Exception as e:
            print("Error:", e)
            print(f"Retrying in {backoff} seconds …")
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)
            continue
        else:
            backoff = 1

        # Wait 60 seconds before fetching again (to simulate continuous updates)
        print("Waiting 60 seconds before next fetch …\n")
        time.sleep(60)

if __name__ == "__main__":
    main()
