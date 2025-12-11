import json
from pathlib import Path
from datetime import datetime
import requests
import logging
import time

# ----------------------------
# Folder setup
# ----------------------------
BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = BASE_DIR / "data" / "raw"
LOG_DIR = BASE_DIR / "logs"

RAW_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(
    filename=LOG_DIR / "extract.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------
# API Endpoints (fictional)
# ----------------------------
DELIVERY_API = "https://api.swiftshipexpress.in/v1/deliveries/live"
TRAFFIC_API = "https://api.swiftshipexpress.in/v1/traffic/routes"


# ---------------------------------------------------------
# Helper: Dummy fallback data (required because API is fake)
# ---------------------------------------------------------
def fallback_delivery_data():
    return {
        "deliveries": [
            {
                "shipment_id": "SH001",
                "source_city": "Hyderabad",
                "destination_city": "Mumbai",
                "dispatch_time": "2025-01-10 08:00",
                "expected_delivery_time": "2025-01-12 20:00",
                "actual_delivery_time": None,
                "package_weight": 14.2,
                "delivery_agent_id": "AG145"
            },
            {
                "shipment_id": "SH002",
                "source_city": "Delhi",
                "destination_city": "Bangalore",
                "dispatch_time": "2025-01-09 10:30",
                "expected_delivery_time": "2025-01-11 18:00",
                "actual_delivery_time": "2025-01-11 19:10",
                "package_weight": 7.8,
                "delivery_agent_id": "AG032"
            }
        ]
    }


def fallback_traffic_data():
    return {
        "routes": [
            {
                "city": "Hyderabad",
                "traffic_congestion_score": 7,
                "avg_speed": 23.5,
                "weather_warning": "Heavy rain"
            },
            {
                "city": "Bangalore",
                "traffic_congestion_score": 6,
                "avg_speed": 28.0,
                "weather_warning": None
            }
        ]
    }


# ---------------------------------------------------------
# Helper: API request with retry
# ---------------------------------------------------------
def make_request(url, retries=3, wait=2):
    for attempt in range(1, retries + 1):
        try:
            print(f"⏳ Requesting: {url} (Attempt {attempt}/{retries})")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            logging.info(f"SUCCESS - API responded: {url}")
            return response.json()
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed. Retrying...")
            logging.error(f"ERROR - API request failed: {url} | Error: {str(e)}")
            time.sleep(wait)

    print(f"❌ API failed after {retries} retries. Using fallback data.")
    logging.warning(f"FALLBACK - Using dummy data for: {url}")

    # Return dummy data
    if "deliveries" in url:
        return fallback_delivery_data()
    if "traffic" in url:
        return fallback_traffic_data()


# ---------------------------------------------------------
# Extract Functions
# ---------------------------------------------------------
def extract_live_delivery_data():
    data = make_request(DELIVERY_API)

    filename = RAW_DIR / f"live_delivery_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filename.write_text(json.dumps(data, indent=2))

    print(f"📦 Saved Live Delivery Data → {filename}")
    logging.info(f"RAW saved: {filename}")
    return str(filename)


def extract_route_traffic_data():
    data = make_request(TRAFFIC_API)

    filename = RAW_DIR / f"route_traffic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filename.write_text(json.dumps(data, indent=2))

    print(f"🚦 Saved Route Traffic Data → {filename}")
    logging.info(f"RAW saved: {filename}")
    return str(filename)


# ---------------------------------------------------------
# Run both extractions
# ---------------------------------------------------------
def extract_all():
    print("🚀 Starting Extraction Pipeline...")

    d1 = extract_live_delivery_data()
    d2 = extract_route_traffic_data()

    print("\n✅ Extraction Completed Successfully!")
    return [d1, d2]


if __name__ == "__main__":
    extract_all()
