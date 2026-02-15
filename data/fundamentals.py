"""
Fetch fundamentals from yfinance and update stock markdown files.
Batch fetches in chunks of 100, falls back to 50 if needed.
"""

import os
import sys
import time
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import yfinance as yf
except ImportError:
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "yfinance"])
    import yfinance as yf

from database import get_db_session, Stock
from data.knowledge import KnowledgeReader


# Fields to extract from yfinance
FUNDAMENTAL_FIELDS = {
    # Valuation
    "trailingPE": "P/E (TTM)",
    "forwardPE": "P/E (Forward)",
    "priceToBook": "P/B",
    "enterpriseToEbitda": "EV/EBITDA",
    "pegRatio": "PEG Ratio",
    
    # Profitability
    "returnOnEquity": "ROE",
    "returnOnAssets": "ROA",
    "profitMargins": "Net Margin",
    "operatingMargins": "Operating Margin",
    
    # Financial Health
    "debtToEquity": "Debt/Equity",
    "currentRatio": "Current Ratio",
    "quickRatio": "Quick Ratio",
    
    # Dividends
    "dividendYield": "Dividend Yield",
    "payoutRatio": "Payout Ratio",
    
    # Size & Growth
    "marketCap": "Market Cap",
    "totalRevenue": "Revenue",
    "revenueGrowth": "Revenue Growth",
    "earningsGrowth": "Earnings Growth",
    
    # Other
    "beta": "Beta",
    "fiftyTwoWeekHigh": "52W High",
    "fiftyTwoWeekLow": "52W Low",
    "freeCashflow": "Free Cash Flow",
}


def format_value(key: str, value: Any) -> str:
    """Format a value for display."""
    if value is None:
        return "N/A"
    
    # Percentages
    if key in ["returnOnEquity", "returnOnAssets", "profitMargins", "operatingMargins",
               "dividendYield", "payoutRatio", "revenueGrowth", "earningsGrowth"]:
        return f"{value * 100:.2f}%"
    
    # Large numbers (crores for Indian context)
    if key in ["marketCap", "totalRevenue", "freeCashflow"]:
        if abs(value) >= 1e7:  # 1 crore
            return f"₹{value / 1e7:.2f} Cr"
        elif abs(value) >= 1e5:  # 1 lakh
            return f"₹{value / 1e5:.2f} L"
        return f"₹{value:,.0f}"
    
    # Prices
    if key in ["fiftyTwoWeekHigh", "fiftyTwoWeekLow"]:
        return f"₹{value:,.2f}"
    
    # Ratios
    if isinstance(value, float):
        return f"{value:.2f}"
    
    return str(value)


def fetch_fundamentals_batch(symbols: List[str], chunk_size: int = 100) -> Dict[str, Dict]:
    """
    Fetch fundamentals for multiple symbols using yfinance.
    
    Args:
        symbols: List of NSE symbols (without .NS suffix)
        chunk_size: Number of symbols per batch
    
    Returns:
        Dict mapping symbol to fundamentals dict
    """
    results = {}
    
    # Convert to yfinance format
    yf_symbols = [f"{sym}.NS" for sym in symbols]
    
    # Chunk the symbols
    chunks = [yf_symbols[i:i + chunk_size] for i in range(0, len(yf_symbols), chunk_size)]
    
    logger.info(f"Fetching fundamentals for {len(symbols)} stocks in {len(chunks)} chunks of {chunk_size}")
    
    for i, chunk in enumerate(chunks):
        try:
            logger.info(f"Fetching chunk {i + 1}/{len(chunks)} ({len(chunk)} stocks)...")
            
            # Batch fetch
            tickers = yf.Tickers(" ".join(chunk))
            
            for yf_sym in chunk:
                orig_sym = yf_sym.replace(".NS", "")
                try:
                    ticker = tickers.tickers.get(yf_sym)
                    if ticker:
                        info = ticker.info
                        if info and info.get("symbol"):
                            results[orig_sym] = {
                                field: info.get(field)
                                for field in FUNDAMENTAL_FIELDS.keys()
                            }
                            results[orig_sym]["_fetched_at"] = datetime.now().isoformat()
                except Exception as e:
                    logger.warning(f"Failed to get info for {orig_sym}: {e}")
            
            # Small delay between chunks to avoid rate limiting
            if i < len(chunks) - 1:
                time.sleep(2)
                
        except Exception as e:
            logger.error(f"Chunk {i + 1} failed with size {chunk_size}: {e}")
            
            # If chunk of 100 fails, try with smaller chunks
            if chunk_size == 100:
                logger.info("Retrying chunk with size 50...")
                chunk_symbols = [s.replace(".NS", "") for s in chunk]
                sub_results = fetch_fundamentals_batch(chunk_symbols, chunk_size=50)
                results.update(sub_results)
            else:
                # Even smaller chunk failed, try individual
                for yf_sym in chunk:
                    orig_sym = yf_sym.replace(".NS", "")
                    try:
                        ticker = yf.Ticker(yf_sym)
                        info = ticker.info
                        if info and info.get("symbol"):
                            results[orig_sym] = {
                                field: info.get(field)
                                for field in FUNDAMENTAL_FIELDS.keys()
                            }
                    except Exception as e2:
                        logger.warning(f"Individual fetch failed for {orig_sym}: {e2}")
    
    logger.success(f"Fetched fundamentals for {len(results)} stocks")
    return results


def generate_metrics_section(fundamentals: Dict) -> str:
    """Generate markdown for Key Metrics section."""
    lines = ["## Key Metrics", ""]
    
    # Group by category
    categories = {
        "Valuation": ["trailingPE", "forwardPE", "priceToBook", "enterpriseToEbitda", "pegRatio"],
        "Profitability": ["returnOnEquity", "returnOnAssets", "profitMargins", "operatingMargins"],
        "Financial Health": ["debtToEquity", "currentRatio", "quickRatio"],
        "Dividends": ["dividendYield", "payoutRatio"],
        "Size": ["marketCap", "totalRevenue", "freeCashflow"],
        "Growth": ["revenueGrowth", "earningsGrowth"],
        "Other": ["beta", "fiftyTwoWeekHigh", "fiftyTwoWeekLow"],
    }
    
    for category, fields in categories.items():
        has_data = any(fundamentals.get(f) is not None for f in fields)
        if has_data:
            lines.append(f"### {category}")
            for field in fields:
                value = fundamentals.get(field)
                if value is not None:
                    label = FUNDAMENTAL_FIELDS.get(field, field)
                    formatted = format_value(field, value)
                    lines.append(f"- **{label}:** {formatted}")
            lines.append("")
    
    # Add timestamp
    if fundamentals.get("_fetched_at"):
        lines.append(f"*Last updated: {fundamentals['_fetched_at'][:10]}*")
    
    return "\n".join(lines)


def update_stock_markdown(symbol: str, fundamentals: Dict, kb: KnowledgeReader) -> bool:
    """Update a stock's markdown file with fundamentals."""
    content = kb.get_stock(symbol)
    if not content:
        logger.warning(f"No markdown file for {symbol}")
        return False
    
    # Generate new metrics section
    new_metrics = generate_metrics_section(fundamentals)
    
    # Replace the Key Metrics section
    if "## Key Metrics" in content:
        # Find the section boundaries
        start = content.find("## Key Metrics")
        
        # Find next section
        next_section = content.find("\n## ", start + 1)
        if next_section == -1:
            next_section = len(content)
        
        # Replace
        new_content = content[:start] + new_metrics + "\n\n" + content[next_section:].lstrip()
    else:
        # Insert after ## Basic Info
        basic_info_end = content.find("\n## ", content.find("## Basic Info") + 1)
        if basic_info_end == -1:
            basic_info_end = len(content)
        new_content = content[:basic_info_end] + "\n\n" + new_metrics + content[basic_info_end:]
    
    return kb.update_stock(symbol, new_content)


def update_all_stocks_with_fundamentals(limit: Optional[int] = None, nifty50_only: bool = False):
    """
    Fetch fundamentals and update all stock markdown files.
    
    Args:
        limit: Max stocks to update (for testing)
        nifty50_only: Only update Nifty 50 stocks
    """
    kb = KnowledgeReader()
    
    # Get symbols from DB
    with get_db_session() as db:
        query = db.query(Stock).filter_by(is_active=True)
        if nifty50_only:
            query = query.filter_by(is_nifty50=True)
        stocks = query.all()
        symbols = [s.symbol for s in stocks]
    
    if limit:
        symbols = symbols[:limit]
    
    logger.info(f"Updating fundamentals for {len(symbols)} stocks")
    
    # Fetch fundamentals in batch
    fundamentals = fetch_fundamentals_batch(symbols, chunk_size=100)
    
    # Update markdown files
    updated = 0
    failed = 0
    
    for symbol in symbols:
        if symbol in fundamentals:
            if update_stock_markdown(symbol, fundamentals[symbol], kb):
                updated += 1
                logger.debug(f"Updated {symbol}")
            else:
                failed += 1
        else:
            failed += 1
            logger.warning(f"No fundamentals for {symbol}")
    
    logger.success(f"Updated {updated} stocks, {failed} failed")
    return updated, failed


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch fundamentals and update markdown files")
    parser.add_argument("--limit", type=int, help="Limit number of stocks (for testing)")
    parser.add_argument("--nifty50", action="store_true", help="Only Nifty 50 stocks")
    parser.add_argument("--symbol", type=str, help="Single symbol to update")
    
    args = parser.parse_args()
    
    if args.symbol:
        # Single stock
        print(f"Fetching fundamentals for {args.symbol}...")
        result = fetch_fundamentals_batch([args.symbol])
        if args.symbol in result:
            print(f"\n{generate_metrics_section(result[args.symbol])}")
            
            kb = KnowledgeReader()
            if update_stock_markdown(args.symbol, result[args.symbol], kb):
                print(f"\n✓ Updated {args.symbol}.md")
        else:
            print(f"Failed to fetch data for {args.symbol}")
    else:
        # Batch update
        print("=" * 50)
        print("Fetching fundamentals from yfinance...")
        print("=" * 50)
        
        updated, failed = update_all_stocks_with_fundamentals(
            limit=args.limit,
            nifty50_only=args.nifty50
        )
        
        print(f"\nDone! Updated: {updated}, Failed: {failed}")
