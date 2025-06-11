#!/usr/bin/env python3
import os
import json
import requests

URL = "https://raw.githubusercontent.com/scam-alert/scam-addresses/main/multichain.json"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "multichain.json")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        resp = requests.get(URL, timeout=30)
        resp.raise_for_status()
        with open(OUTPUT_FILE, "w") as f:
            json.dump(resp.json(), f, indent=2)
        print(f"Updated scammer list at {OUTPUT_FILE}")
    except Exception as e:
        print(f"Failed to update scammer list: {e}")

if __name__ == "__main__":
    main()
