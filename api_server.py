from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from bson import ObjectId
import uvicorn
import subprocess
import os

# -----------------------------
# FASTAPI APP SETUP
# -----------------------------
app = FastAPI(
    title="SENAPRED Chile Tweet Monitoring API",
    version="1.0.0",
    description="API for real-time Chile emergency analytics and tweet streaming."
)

# -----------------------------
# CORS (Frontend will need this)
# -----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow any frontend temporarily
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# MONGODB CONNECTION
# -----------------------------
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "tweets_chile"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Helper: convert ObjectId to string
def to_json(doc):
    doc["_id"] = str(doc["_id"])
    return doc

# ============================================================
# 1️⃣ API: Get latest analytics
# ============================================================
@app.get("/analytics/latest")
def get_latest_analytics():
    latest = db.analytics_results.find_one(sort=[("_id", -1)])
    if not latest:
        return {"message": "No analytics found."}
    return to_json(latest)

# ============================================================
# 2️⃣ API: Get counts by location
# ============================================================
@app.get("/analytics/locations")
def get_location_counts():
    latest = db.analytics_results.find_one(sort=[("_id", -1)])
    if not latest:
        return {"message": "No analytics found."}
    return latest.get("counts_by_location", {})

# ============================================================
# 3️⃣ API: Get counts by keyword
# ============================================================
@app.get("/analytics/keywords")
def get_keyword_counts():
    latest = db.analytics_results.find_one(sort=[("_id", -1)])
    if not latest:
        return {"message": "No analytics found."}
    return latest.get("counts_by_keyword", {})

# ============================================================
# 4️⃣ API: Get counts by hour
# ============================================================
@app.get("/analytics/hours")
def get_hour_counts():
    latest = db.analytics_results.find_one(sort=[("_id", -1)])
    if not latest:
        return {"message": "No analytics found."}
    return latest.get("counts_by_hour", {})

# ============================================================
# 5️⃣ API: Trigger analytics script manually
# ============================================================
@app.get("/analytics/run")
def run_analytics():
    try:
        subprocess.run(["python", "analytics_chile_tweets.py"], check=True)
        return {"status": "success", "message": "Analytics script executed."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ============================================================
# 6️⃣ API: Start streaming (synthetic Chile generator)
# ============================================================
stream_process = None

@app.get("/stream/start")
def start_stream():
    global stream_process

    if stream_process and stream_process.poll() is None:
        return {"status": "running", "message": "Stream already running."}

    stream_process = subprocess.Popen(["python", "synthetic_chile_stream.py"])
    return {"status": "started", "message": "Streaming started."}

# ============================================================
# 7️⃣ API: Get stream status
# ============================================================
@app.get("/stream/status")
def stream_status():
    if stream_process and stream_process.poll() is None:
        return {"stream_running": True}
    return {"stream_running": False}

# ============================================================
# 8️⃣ API: Stop stream
# ============================================================
@app.get("/stream/stop")
def stop_stream():
    global stream_process

    if stream_process:
        stream_process.terminate()
        return {"status": "stopped"}

    return {"status": "not_running"}

# ============================================================
# START SERVER
# ============================================================
if __name__ == "__main__":
    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)
