import urllib.request
import json
import os
from datetime import datetime, timezone

FRED_API_KEY = os.environ.get('FRED_API_KEY', '')

def fetch_fred(series_id, limit=14):
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}"
        f"&api_key={FRED_API_KEY}"
        f"&sort_order=desc"
        f"&limit={limit}"
        f"&file_type=json"
    )
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            raw = json.loads(response.read().decode())
            # Filter out missing values (FRED uses '.' for missing)
            obs = [o for o in raw.get('observations', []) if o['value'] != '.']
            return obs
    except Exception as e:
        print(f"FRED fetch failed for {series_id}: {e}")
        return []

def build_output():
    print("Fetching FRED WPU011306 (Potatoes PPI)...")
    potato_obs = fetch_fred('WPU011306', limit=14)

    print("Fetching FRED PBARLUSDA (Barley annual)...")
    barley_obs = fetch_fred('PBARLUSDA', limit=5)

    output = {
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "potato": {
            "source": "FRED WPU011306 — BLS Producer Price Index, Potatoes (monthly, ~4 wk lag)",
            "seriesId": "WPU011306",
            "fredUrl": "https://fred.stlouisfed.org/series/WPU011306",
            "data": potato_obs
        },
        "barley": {
            "source": "FRED PBARLUSDA — USDA Annual Average Farm Price, Barley",
            "seriesId": "PBARLUSDA",
            "fredUrl": "https://fred.stlouisfed.org/series/PBARLUSDA",
            "data": barley_obs
        }
    }

    os.makedirs('data', exist_ok=True)
    with open('data/market-data.json', 'w') as f:
        json.dump(output, f, indent=2)

    print(f"Written data/market-data.json — potato: {len(potato_obs)} obs, barley: {len(barley_obs)} obs")

if __name__ == '__main__':
    build_output()
