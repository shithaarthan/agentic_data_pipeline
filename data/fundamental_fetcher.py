"""
Fundamental Data Fetcher
Fetches comprehensive fundamental data from multiple sources with caching.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
import httpx
from bs4 import BeautifulSoup
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings

# Cache directory
CACHE_DIR = Path(__file__).parent.parent / "data" / "fundamental_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# FMP API limit tracking
FMP_LIMIT_FILE = CACHE_DIR / "fmp_usage.json"


class FMPRateLimiter:
    """Track FMP API usage to stay within 250 calls/day limit."""
    
    def __init__(self):
        self.limit_file = FMP_LIMIT_FILE
        self.daily_limit = 250
        
    def _load_usage(self) -> Dict:
        """Load today's usage."""
        if self.limit_file.exists():
            with open(self.limit_file) as f:
                data = json.load(f)
                # Reset if it's a new day
                if data.get('date') != datetime.now().strftime('%Y-%m-%d'):
                    return {'date': datetime.now().strftime('%Y-%m-%d'), 'count': 0}
                return data
        return {'date': datetime.now().strftime('%Y-%m-%d'), 'count': 0}
    
    def _save_usage(self, data: Dict):
        """Save usage data."""
        with open(self.limit_file, 'w') as f:
            json.dump(data, f)
    
    def can_call(self) -> bool:
        """Check if we can make an API call."""
        usage = self._load_usage()
        return usage['count'] < self.daily_limit
    
    def increment(self):
        """Increment usage counter."""
        usage = self._load_usage()
        usage['count'] += 1
        self._save_usage(usage)
        logger.debug(f"FMP API calls today: {usage['count']}/{self.daily_limit}")
    
    def remaining(self) -> int:
        """Get remaining calls."""
        usage = self._load_usage()
        return self.daily_limit - usage['count']


class FundamentalDataFetcher:
    """
    Fetches fundamental data from multiple sources with intelligent caching.
    Priority: Screener.in (free) > FMP (limited) > yfinance (basic)
    """
    
    def __init__(self):
        self.fmp_limiter = FMPRateLimiter()
        self.fmp_api_key = os.getenv('FMP_API_KEY', '')
        self.cache_validity_days = 7  # Cache valid for 7 days
        
    def _get_cache_path(self, symbol: str, source: str) -> Path:
        """Get cache file path for a symbol and source."""
        return CACHE_DIR / f"{symbol}_{source}.json"
    
    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache is still valid."""
        if not cache_path.exists():
            return False
        
        # Check age
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age = datetime.now() - mtime
        return age.days < self.cache_validity_days
    
    def _load_cache(self, symbol: str, source: str) -> Optional[Dict]:
        """Load cached data if valid."""
        cache_path = self._get_cache_path(symbol, source)
        if self._is_cache_valid(cache_path):
            try:
                with open(cache_path) as f:
                    logger.debug(f"Using cached {source} data for {symbol}")
                    return json.load(f)
            except:
                pass
        return None
    
    def _save_cache(self, symbol: str, source: str, data: Dict):
        """Save data to cache."""
        cache_path = self._get_cache_path(symbol, source)
        with open(cache_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def fetch_screener_data(self, symbol: str) -> Optional[Dict]:
        """
        Scrape data from Screener.in (unlimited, free).
        Returns: Company profile, quarterly results, ratios, peers.
        """
        # Check cache first
        cached = self._load_cache(symbol, 'screener')
        if cached:
            return cached
        
        url = f"https://www.screener.in/company/{symbol}/consolidated/"
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            with httpx.Client(timeout=15.0, follow_redirects=True) as client:
                response = client.get(url, headers=headers)
                response.raise_for_status()
                
            soup = BeautifulSoup(response.text, 'lxml')
            
            data = {
                'symbol': symbol,
                'source': 'screener.in',
                'fetched_at': datetime.now().isoformat(),
            }
            
            # Company name
            name_elem = soup.find('h1', class_='h2')
            if name_elem:
                data['company_name'] = name_elem.text.strip()
            
            # Market cap, current price
            top_ratios = soup.find('ul', id='top-ratios')
            if top_ratios:
                ratios = {}
                for li in top_ratios.find_all('li'):
                    name = li.find('span', class_='name')
                    value = li.find('span', class_='number')
                    if name and value:
                        ratios[name.text.strip()] = value.text.strip()
                data['top_ratios'] = ratios
            
            # Quarterly results (last 4 quarters)
            quarterly_table = soup.find('section', id='quarters')
            if quarterly_table:
                quarters = []
                table = quarterly_table.find('table')
                if table:
                    headers = [th.text.strip() for th in table.find_all('th')]
                    for row in table.find_all('tr')[1:5]:  # Last 4 quarters
                        cells = [td.text.strip() for td in row.find_all('td')]
                        if cells:
                            quarters.append(dict(zip(headers, cells)))
                data['quarterly_results'] = quarters
            
            # Peer comparison
            peers_section = soup.find('section', id='peers')
            if peers_section:
                peers = []
                table = peers_section.find('table')
                if table:
                    for row in table.find_all('tr')[1:6]:  # Top 5 peers
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            peers.append({
                                'name': cells[0].text.strip(),
                                'market_cap': cells[1].text.strip() if len(cells) > 1 else 'N/A'
                            })
                data['peers'] = peers
            
            # Save to cache
            self._save_cache(symbol, 'screener', data)
            logger.success(f"Fetched Screener.in data for {symbol}")
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch Screener.in data for {symbol}: {e}")
            return None
    
    def fetch_fmp_data(self, symbol: str) -> Optional[Dict]:
        """
        Fetch data from FMP API (250 calls/day limit).
        Returns: Company profile, financial ratios, growth metrics.
        """
        # Check cache first
        cached = self._load_cache(symbol, 'fmp')
        if cached:
            return cached
        
        # Check rate limit
        if not self.fmp_limiter.can_call():
            logger.warning(f"FMP API limit reached. Remaining: {self.fmp_limiter.remaining()}")
            return None
        
        if not self.fmp_api_key:
            logger.warning("FMP API key not configured")
            return None
        
        # FMP uses different symbol format for Indian stocks
        fmp_symbol = f"{symbol}.NS"  # NSE stocks
        
        try:
            base_url = "https://financialmodelingprep.com/api/v3"
            
            data = {
                'symbol': symbol,
                'source': 'fmp',
                'fetched_at': datetime.now().isoformat(),
            }
            
            with httpx.Client(timeout=15.0) as client:
                # Company profile
                profile_url = f"{base_url}/profile/{fmp_symbol}?apikey={self.fmp_api_key}"
                profile_resp = client.get(profile_url)
                if profile_resp.status_code == 200:
                    profile_data = profile_resp.json()
                    if profile_data:
                        data['profile'] = profile_data[0]
                        self.fmp_limiter.increment()
                
                # Financial ratios
                ratios_url = f"{base_url}/ratios/{fmp_symbol}?apikey={self.fmp_api_key}"
                ratios_resp = client.get(ratios_url)
                if ratios_resp.status_code == 200:
                    ratios_data = ratios_resp.json()
                    if ratios_data:
                        data['ratios'] = ratios_data[0]  # Latest year
                        self.fmp_limiter.increment()
                
                # Growth metrics
                growth_url = f"{base_url}/financial-growth/{fmp_symbol}?apikey={self.fmp_api_key}"
                growth_resp = client.get(growth_url)
                if growth_resp.status_code == 200:
                    growth_data = growth_resp.json()
                    if growth_data:
                        data['growth'] = growth_data[0]  # Latest year
                        self.fmp_limiter.increment()
            
            # Save to cache
            self._save_cache(symbol, 'fmp', data)
            logger.success(f"Fetched FMP data for {symbol} (API calls remaining: {self.fmp_limiter.remaining()})")
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch FMP data for {symbol}: {e}")
            return None
    
    def fetch_all(self, symbol: str) -> Dict:
        """
        Fetch all available fundamental data for a symbol.
        Priority: Screener.in (always) > FMP (if quota available)
        """
        logger.info(f"Fetching fundamental data for {symbol}...")
        
        result = {
            'symbol': symbol,
            'screener': None,
            'fmp': None,
            'fetched_at': datetime.now().isoformat()
        }
        
        # Always try Screener.in (free, unlimited)
        screener_data = self.fetch_screener_data(symbol)
        if screener_data:
            result['screener'] = screener_data
        
        # Try FMP if quota available
        if self.fmp_limiter.remaining() > 10:  # Keep buffer of 10 calls
            fmp_data = self.fetch_fmp_data(symbol)
            if fmp_data:
                result['fmp'] = fmp_data
        else:
            logger.info(f"Skipping FMP for {symbol} (quota low: {self.fmp_limiter.remaining()} remaining)")
        
        return result


# CLI for testing
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fundamental Data Fetcher")
    parser.add_argument("symbol", type=str, help="Stock symbol")
    parser.add_argument("--source", choices=['screener', 'fmp', 'all'], default='all')
    
    args = parser.parse_args()
    
    fetcher = FundamentalDataFetcher()
    
    if args.source == 'screener':
        data = fetcher.fetch_screener_data(args.symbol.upper())
    elif args.source == 'fmp':
        data = fetcher.fetch_fmp_data(args.symbol.upper())
    else:
        data = fetcher.fetch_all(args.symbol.upper())
    
    print(json.dumps(data, indent=2))
