import urllib.request
import urllib.error
import json
import os
import time
from datetime import datetime, timezone

FRED_API_KEY = os.environ.get('FRED_API_KEY', '')

# ── Yahoo Finance fetch (server-side, no CORS issues) ──
def fetch_yahoo(symbol, range_param='1y', interval='1mo'):
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{symbol}?range={range_param}&interval={interval}"
        f"&includePrePost=false"
    )
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = json.loads(response.read().decode())
            result = raw.get('chart', {}).get('result', [None])[0]
            if not result:
                raise ValueError(f"No result for {symbol}")
            return result
    except Exception as e:
        print(f"Yahoo fetch failed for {symbol}: {e}")
        return None


def extract_chart_data(result, symbol):
    if not result:
        return {'symbol': symbol, 'error': True, 'errorMsg': 'fetch failed', 'pairs': []}

    meta       = result.get('meta', {})
    timestamps = result.get('timestamp', [])
    quote      = result.get('indicators', {}).get('quote', [{}])[0]
    closes     = quote.get('close', [])
    opens      = quote.get('open',  [])
    highs      = quote.get('high',  [])
    lows       = quote.get('low',   [])

    pairs = []
    for i, t in enumerate(timestamps):
        if i < len(closes) and closes[i] is not None:
            pairs.append({
                't': t,
                'o': round(opens[i],  4) if i < len(opens)  and opens[i]  is not None else None,
                'h': round(highs[i],  4) if i < len(highs)  and highs[i]  is not None else None,
                'l': round(lows[i],   4) if i < len(lows)   and lows[i]   is not None else None,
                'c': round(closes[i], 4),
            })

    current_price = meta.get('regularMarketPrice')
    prev_close    = meta.get('chartPreviousClose') or meta.get('previousClose')
    change        = None
    pct           = None
    if current_price is not None and prev_close is not None and prev_close != 0:
        change = round(current_price - prev_close, 4)
        pct    = round((change / prev_close) * 100, 2)

    return {
        'symbol':       symbol,
        'error':        False,
        'currentPrice': current_price,
        'prevClose':    prev_close,
        'change':       change,
        'pct':          pct,
        'marketState':  meta.get('marketState', ''),
        'pairs':        pairs,
    }


# ── FRED fetch ──
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
            obs = [o for o in raw.get('observations', []) if o['value'] != '.']
            return obs
    except Exception as e:
        print(f"FRED fetch failed for {series_id}: {e}")
        return []


def convert_pairs(pairs, divisor):
    """Convert price pairs by divisor (for cents-to-dollars conversion)."""
    result = []
    for pt in pairs:
        result.append({
            't': pt['t'],
            'c': round(pt['c'] / divisor, 4),
            'o': round(pt['o'] / divisor, 4) if pt.get('o') is not None else None,
            'h': round(pt['h'] / divisor, 4) if pt.get('h') is not None else None,
            'l': round(pt['l'] / divisor, 4) if pt.get('l') is not None else None,
        })
    return result


def convert_price(p, divisor):
    if p is None:
        return None
    return round(p / divisor, 4)


def build_output():
    # ── Commodity symbols ──
    # is_cents=True  → Yahoo returns price in cents/bu (grains)
    # is_cents=False → Yahoo returns price in dollars (livestock)
    SYMBOLS = [
        ('ZC=F', 'Corn',           'CBOT', '/bu',  True),
        ('ZS=F', 'Soybeans',       'CBOT', '/bu',  True),
        ('ZW=F', 'Soft Wht Wheat', 'CBOT', '/bu',  True),
        ('KE=F', 'Hard Red Wheat', 'KCBT', '/bu',  True),
        ('LE=F', 'Live Cattle',    'CME',  '/cwt', False),
        ('HE=F', 'Lean Hogs',      'CME',  '/cwt', False),
    ]

    commodities = {}

    for symbol, name, exchange, unit, is_cents in SYMBOLS:
        divisor = 100.0 if is_cents else 1.0

        print(f"Fetching Yahoo Finance daily:   {symbol}...")
        daily_result = fetch_yahoo(symbol, range_param='1d', interval='5m')
        time.sleep(0.5)

        print(f"Fetching Yahoo Finance monthly: {symbol}...")
        monthly_result = fetch_yahoo(symbol, range_param='1y', interval='1mo')
        time.sleep(0.5)

        daily_data   = extract_chart_data(daily_result,   symbol)
        monthly_data = extract_chart_data(monthly_result, symbol)

        current_price = convert_price(daily_data.get('currentPrice'), divisor)
        prev_close    = convert_price(daily_data.get('prevClose'),    divisor)
        change        = convert_price(daily_data.get('change'),       divisor)
        pct           = daily_data.get('pct')  # percentage ratio unchanged

        commodities[symbol] = {
            'name':         name,
            'exchange':     exchange,
            'unit':         unit,
            'currentPrice': current_price,
            'prevClose':    prev_close,
            'change':       change,
            'pct':          pct,
            'marketState':  daily_data.get('marketState', ''),
            'error':        daily_data.get('error', False),
            'errorMsg':     daily_data.get('errorMsg', ''),
            'dailyPairs':   convert_pairs(daily_data.get('pairs',   []), divisor),
            'monthlyPairs': convert_pairs(monthly_data.get('pairs', []), divisor),
        }

        print(f"  -> price=${current_price}  change={change}  pct={pct}%  "
              f"daily_pts={len(commodities[symbol]['dailyPairs'])}  "
              f"monthly_pts={len(commodities[symbol]['monthlyPairs'])}")

    # ── FRED ──
    print("\nFetching FRED WPU011306 (Potatoes PPI)...")
    potato_obs = fetch_fred('WPU011306', limit=14)

    print("Fetching FRED PBARLUSDA (Barley annual)...")
    barley_obs = fetch_fred('PBARLUSDA', limit=5)

    output = {
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "commodities": commodities,
        "potato": {
            "source":   "FRED WPU011306 — BLS Producer Price Index, Potatoes (monthly, ~4 wk lag)",
            "seriesId": "WPU011306",
            "fredUrl":  "https://fred.stlouisfed.org/series/WPU011306",
            "data":     potato_obs,
        },
        "barley": {
            "source":   "FRED PBARLUSDA — USDA Annual Average Farm Price, Barley",
            "seriesId": "PBARLUSDA",
            "fredUrl":  "https://fred.stlouisfed.org/series/PBARLUSDA",
            "data":     barley_obs,
        },
    }

    os.makedirs('data', exist_ok=True)
    with open('data/market-data.json', 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nDone. Written data/market-data.json")
    print(f"  Commodities: {len(commodities)}")
    print(f"  Potato obs:  {len(potato_obs)}")
    print(f"  Barley obs:  {len(barley_obs)}")


if __name__ == '__main__':
    build_output()
