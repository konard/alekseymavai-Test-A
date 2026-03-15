#!/usr/bin/env python3
"""
Fetch real 30-day volume data for Uniswap v3 pools from GeckoTerminal.
Calculates APY using formula:
  APY = (Fees_30d / TVL) * (365/30) * 100%
  where Fees_30d = Volume_30d * (fee_tier / 100)

No external dependencies - uses only standard library.
"""

import urllib.request
import urllib.parse
import urllib.error
import json
import time
import os
from datetime import datetime

STABLECOINS = {
    'USDT', 'USDC', 'DAI', 'BUSD', 'FRAX', 'USDD', 'TUSD', 'USDP',
    'USDE', 'USDS', 'PYUSD', 'RLUSD', 'USDG', 'USYC', 'USDF',
    'BSC-USD', 'SUSDS', 'SUSDE', 'BFUSD', 'USD1', 'USDT0',
    'SYRUPUSDC', 'GUSD', 'LUSD', 'SUSD', 'FDUSD', 'CUSD'
}
MAJOR_COINS = {
    'BTC', 'WBTC', 'CBBTC', 'FBTC', 'TBTC', 'HBTC', 'RENBTC',
    'ETH', 'WETH', 'STETH', 'WSTETH', 'RETH', 'CBETH', 'WBETH',
    'WEETH', 'BETH', 'SFRXETH', 'FRXETH', 'EETH', 'RSETH'
}

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (compatible; Python/3.12)'
}

def load_top_100_symbols():
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(script_dir, 'top_100_coins_symbols.json')
        with open(path, 'r') as f:
            return set(s.upper() for s in json.load(f))
    except FileNotFoundError:
        print("Warning: top_100_coins_symbols.json not found")
        return set()

TOP_100_SYMBOLS = load_top_100_symbols()
ALLOWED = STABLECOINS | MAJOR_COINS | TOP_100_SYMBOLS

def http_get(url, params=None, retries=3):
    if params:
        url = url + '?' + urllib.parse.urlencode(params)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 10 * (attempt + 1)
                print(f"    Rate limited (429). Waiting {wait}s...")
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(3)
    return None

def parse_pool_name(pool_name):
    parts = pool_name.split('/')
    if len(parts) < 2:
        return None, None, None
    token1 = parts[0].strip().upper()
    token2_parts = parts[1].strip().split()
    if len(token2_parts) < 2:
        return None, None, None
    token2 = token2_parts[0].upper()
    try:
        fee_tier = float(token2_parts[1].replace('%', ''))
    except ValueError:
        return None, None, None
    return token1, token2, fee_tier

def is_allowed_pool(pool_name):
    t1, t2, _ = parse_pool_name(pool_name)
    if not t1 or not t2:
        return False
    return t1 in ALLOWED and t2 in ALLOWED

def fetch_pools_page(page):
    url = "https://api.geckoterminal.com/api/v2/networks/eth/dexes/uniswap_v3/pools"
    try:
        return http_get(url, {"page": page})
    except Exception as e:
        print(f"  Error fetching page {page}: {e}")
        return None

def fetch_30d_ohlcv(pool_address):
    """Fetch 30 days of daily OHLCV data from GeckoTerminal"""
    url = f"https://api.geckoterminal.com/api/v2/networks/eth/pools/{pool_address}/ohlcv/day"
    params = {"aggregate": "1", "limit": "30", "currency": "usd"}
    try:
        data = http_get(url, params)
        if data:
            ohlcv = data['data']['attributes']['ohlcv_list']
            return ohlcv
        return None
    except Exception as e:
        print(f"    Error: {e}")
        return None

def main():
    print("=" * 70)
    print("Fetching real 30-day volume data for Uniswap v3 pools")
    print("Source: GeckoTerminal daily OHLCV API")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)
    print(f"Allowed tokens: {len(ALLOWED)} ({len(STABLECOINS)} stablecoins + "
          f"{len(MAJOR_COINS)} BTC/ETH + {len(TOP_100_SYMBOLS)} top-100)")
    
    all_pools = []
    checked = 0
    filtered_count = 0
    
    for page in range(1, 11):
        data = fetch_pools_page(page)
        if not data or 'data' not in data:
            break
        
        print(f"\nPage {page}: {len(data['data'])} pools")
        time.sleep(2)  # Delay between pages
        
        for pool in data['data']:
            try:
                name = pool['attributes']['name']
                checked += 1
                
                if not is_allowed_pool(name):
                    continue
                
                t1, t2, fee_tier = parse_pool_name(name)
                if fee_tier is None:
                    continue
                
                reserve_usd = float(pool['attributes']['reserve_in_usd'])
                if reserve_usd < 100_000:
                    continue
                
                filtered_count += 1
                address = pool['attributes']['address']
                volume_24h = float(pool['attributes']['volume_usd']['h24'])
                
                print(f"  [{filtered_count}] {name} TVL=${reserve_usd:,.0f}", flush=True)
                print(f"    Fetching 30d OHLCV...", end=' ', flush=True)
                
                time.sleep(2)  # Rate limiting
                ohlcv = fetch_30d_ohlcv(address)
                
                if ohlcv and len(ohlcv) > 0:
                    volume_30d = sum(candle[5] for candle in ohlcv)
                    days_of_data = len(ohlcv)
                    has_real_30d = days_of_data >= 25
                    print(f"30d vol: ${volume_30d:,.0f} ({days_of_data} days)")
                else:
                    volume_30d = volume_24h * 30
                    days_of_data = 0
                    has_real_30d = False
                    print(f"no data, estimate: ${volume_30d:,.0f}")
                
                fees_24h = volume_24h * (fee_tier / 100)
                apy_24h = (fees_24h / reserve_usd) * 365 * 100 if reserve_usd > 0 else 0
                
                fees_30d = volume_30d * (fee_tier / 100)
                apy_30d = (fees_30d / reserve_usd) * (365 / 30) * 100 if reserve_usd > 0 else 0
                
                all_pools.append({
                    'name': name,
                    'address': address,
                    'reserve_usd': reserve_usd,
                    'volume_24h': volume_24h,
                    'volume_30d': volume_30d,
                    'days_of_data': days_of_data,
                    'fee_tier': fee_tier,
                    'apy_24h': round(apy_24h, 4),
                    'apy_30d': round(apy_30d, 4),
                    'has_real_30d_data': has_real_30d,
                    'pool_created_at': pool['attributes']['pool_created_at'],
                    'data_source': 'GeckoTerminal OHLCV (real 30-day data)' if has_real_30d else 'GeckoTerminal (estimated)'
                })
                
            except Exception as e:
                print(f"  Error processing {pool.get('attributes', {}).get('name', 'N/A')}: {e}")
        
        time.sleep(3)  # Delay between pages
    
    print(f"\n{'='*70}")
    print(f"Summary: checked={checked}, passed_filter={filtered_count}, results={len(all_pools)}")
    
    all_pools.sort(key=lambda x: x['apy_30d'], reverse=True)
    top_20 = all_pools[:20]
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, 'top_20_pools_with_real_30d_data.json')
    with open(output_path, 'w') as f:
        json.dump(top_20, f, indent=2)
    print(f"Saved top-20 to: {output_path}")
    
    print(f"\n{'='*70}")
    print("TOP-20 POOLS BY REAL 30-DAY APY")
    print(f"{'='*70}")
    print(f"{'#':<3} {'Pool':<30} {'APY 24h':>10} {'APY 30d':>10} {'Vol 30d':>22} {'TVL':>20}")
    for i, p in enumerate(top_20, 1):
        marker = "✅" if p['has_real_30d_data'] else "⚠️"
        print(f"{i:<3} {p['name']:<30} {p['apy_24h']:>9.2f}% {p['apy_30d']:>9.2f}% "
              f"${p['volume_30d']:>19,.0f}  ${p['reserve_usd']:>15,.0f} {marker}")
    
    return top_20

if __name__ == "__main__":
    main()
