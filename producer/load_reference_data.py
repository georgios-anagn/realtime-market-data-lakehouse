# A one-off script to upload to "Catalog/Volumes/market_pipeline/landing/reference" a "company_profiles.json" file with the companies' details
import json
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

from uploader import upload_file

load_dotenv()

FINNHUB_TOKEN = os.environ["FINNHUB_API_KEY"]
SYMBOLS = os.environ.get("SYMBOLS", "AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA,META").split(",")
VOLUME_PATH = os.environ["DATABRICKS_REFERENCE_VOLUME_PATH"]


def fetch_profile(symbol: str) -> dict:
    resp = requests.get(
        "https://finnhub.io/api/v1/stock/profile2",
        params={"symbol": symbol, "token": FINNHUB_TOKEN},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def main():
    local_file = Path("data/reference/company_profiles.json")
    local_file.parent.mkdir(parents=True, exist_ok=True)

    with open(local_file, "w") as f:
        for symbol in SYMBOLS:
            profile = fetch_profile(symbol)
            profile["symbol"] = symbol
            f.write(json.dumps(profile) + "\n")
            time.sleep(1)  # stay comfortably under the free-tier rate limit

    upload_file(local_file, f"{VOLUME_PATH}/company_profiles.json")
    print("Uploaded company profiles")


if __name__ == "__main__":
    main()