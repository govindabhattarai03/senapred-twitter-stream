"""Central configuration.

Every secret and environment-specific value is read from environment variables
(or a local `.env` file), so nothing sensitive is ever committed to Git.
"""
import os

from dotenv import load_dotenv

load_dotenv()

# --- MongoDB ---------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "senapred")

# --- X (Twitter) API ---------------------------------------------------------
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")  # never hard-code this
X_SEARCH_URL = "https://api.x.com/2/tweets/search/recent"
X_QUERY = os.getenv(
    "X_QUERY",
    "(terremoto OR sismo OR incendio OR tsunami OR emergencia OR alerta OR evacuación OR lluvias) "
    "(Chile OR Santiago OR Valparaíso OR Concepción) lang:es -is:retweet",
)
# The free/basic X API tiers are heavily rate-limited, so poll slowly.
X_POLL_SECONDS = int(os.getenv("X_POLL_SECONDS", "900"))

# --- Synthetic generator -----------------------------------------------------
SYNTHETIC_INTERVAL_SECONDS = float(os.getenv("SYNTHETIC_INTERVAL_SECONDS", "5"))

# --- Domain vocabulary -------------------------------------------------------
EMERGENCY_KEYWORDS = [
    "terremoto", "sismo", "incendio", "tsunami",
    "emergencia", "alerta", "evacuación", "lluvias",
]

CHILE_LOCATIONS = [
    "Santiago", "Valparaíso", "Concepción", "La Serena",
    "Antofagasta", "Temuco", "Puerto Montt", "Iquique",
]
