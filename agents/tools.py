"""
MCP Tools for Trading Agents
These functions are exposed to LLM agents for market analysis.
"""

import os
import sys
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.market_data import MarketData
from data.technical_indicators import TechnicalAnalyzer
from data.knowledge import KnowledgeReader
from database import get_db_session, Stock


# Initialize shared instances
_market_data: Optional[MarketData] = None
_knowledge: Optional[KnowledgeReader] = None


def _get_market_data() -> MarketData:
    global _market_data
    if _market_data is None:
        _market_data = MarketData()
    return _market_data


def _get_knowledge() -> KnowledgeReader:
    global _knowledge
    if _knowledge is None:
        _knowledge = KnowledgeReader()
    return _knowledge


# ============================================
# MARKET DATA TOOLS
# ============================================

def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Get current quote for a stock.
    
    Args:
        symbol: Stock symbol (e.g., "RELIANCE", "TCS")
    
    Returns:
        Dict with ltp, open, high, low, close, volume, change, change_pct
    """
    md = _get_market_data()
    quote = md.get_quote(symbol.upper())
    
    if not quote:
        return {"error": f"Could not fetch quote for {symbol}"}
    
    return {
        "symbol": quote.symbol,
        "ltp": quote.ltp,
        "open": quote.open,
        "high": quote.high,
        "low": quote.low,
        "close": quote.close,
        "volume": quote.volume,
        "change": quote.change,
        "change_pct": quote.change_pct,
        "timestamp": quote.timestamp.isoformat() if quote.timestamp else None
    }


def get_historical(symbol: str, days: int = 60) -> Dict[str, Any]:
    """
    Get historical OHLCV data for a stock.
    
    Args:
        symbol: Stock symbol
        days: Number of days of history (default 60)
    
    Returns:
        Dict with dates, open, high, low, close, volume arrays
    """
    md = _get_market_data()
    df = md.get_historical(symbol.upper(), days=days)
    
    if df is None or df.empty:
        return {"error": f"Could not fetch historical data for {symbol}"}
    
    return {
        "symbol": symbol.upper(),
        "days": len(df),
        "data": {
            "dates": df.index.strftime("%Y-%m-%d").tolist(),
            "open": df["open"].round(2).tolist(),
            "high": df["high"].round(2).tolist(),
            "low": df["low"].round(2).tolist(),
            "close": df["close"].round(2).tolist(),
            "volume": df["volume"].astype(int).tolist()
        }
    }


def get_technicals(symbol: str, days: int = 100) -> Dict[str, Any]:
    """
    Get technical analysis for a stock.
    
    Args:
        symbol: Stock symbol
        days: Days of data to analyze
    
    Returns:
        Dict with RSI, MACD, moving averages, signals, etc.
    """
    md = _get_market_data()
    df = md.get_historical(symbol.upper(), days=days)
    
    if df is None or df.empty:
        return {"error": f"Could not fetch data for {symbol}"}
    
    ta = TechnicalAnalyzer(df)
    summary = ta.get_full_analysis(symbol.upper())
    
    return {
        "symbol": symbol.upper(),
        "overall_signal": summary.overall_signal.value,
        "bullish_signals": summary.bullish_count,
        "bearish_signals": summary.bearish_count,
        "indicators": {
            name: {
                "value": round(ind.value, 2),
                "signal": ind.signal.value,
                "description": ind.description
            }
            for name, ind in summary.indicators.items()
        },
        "analysis_text": summary.analysis_text
    }


# ============================================
# KNOWLEDGE BASE TOOLS
# ============================================

def get_stock_info(symbol: str) -> Dict[str, Any]:
    """
    Get knowledge file content for a stock.
    
    Args:
        symbol: Stock symbol
    
    Returns:
        Dict with stock knowledge markdown content
    """
    kb = _get_knowledge()
    content = kb.get_stock(symbol.upper())
    
    if not content:
        # Get basic info from DB
        with get_db_session() as db:
            stock = db.query(Stock).filter_by(symbol=symbol.upper()).first()
            if stock:
                return {
                    "symbol": stock.symbol,
                    "name": stock.name,
                    "sector": stock.sector,
                    "industry": stock.industry,
                    "knowledge_file_exists": False
                }
        return {"error": f"No information found for {symbol}"}
    
    return {
        "symbol": symbol.upper(),
        "knowledge": content,
        "knowledge_file_exists": True
    }


def get_sector_info(sector: str) -> Dict[str, Any]:
    """
    Get knowledge file content for a sector.
    
    Args:
        sector: Sector name (e.g., "IT", "Banking", "Pharma")
    
    Returns:
        Dict with sector knowledge markdown content
    """
    kb = _get_knowledge()
    content = kb.get_sector(sector)
    
    if not content:
        return {"error": f"No sector knowledge found for {sector}"}
    
    return {
        "sector": sector,
        "knowledge": content
    }


def search_knowledge(query: str, limit: int = 5) -> List[Dict[str, str]]:
    """
    Search across all knowledge files.
    
    Args:
        query: Search term
        limit: Max results
    
    Returns:
        List of matching files with snippets
    """
    kb = _get_knowledge()
    return kb.search(query, limit=limit)


def get_strategy(strategy_name: str) -> Dict[str, Any]:
    """
    Get a trading strategy description.
    
    Args:
        strategy_name: Name like "breakout", "swing_trading"
    
    Returns:
        Strategy knowledge content
    """
    kb = _get_knowledge()
    content = kb.get_strategy(strategy_name)
    
    if not content:
        return {"error": f"Strategy '{strategy_name}' not found"}
    
    return {
        "strategy": strategy_name,
        "knowledge": content
    }


# ============================================
# STOCK UNIVERSE TOOLS
# ============================================

def list_stocks(index: str = "nifty50", sector: Optional[str] = None) -> List[Dict[str, str]]:
    """
    List stocks from an index or sector.
    
    Args:
        index: "nifty50", "nifty100", "nifty500", or "all"
        sector: Optional sector filter
    
    Returns:
        List of stocks with symbol, name, sector
    """
    with get_db_session() as db:
        query = db.query(Stock).filter_by(is_active=True)
        
        if index == "nifty50":
            query = query.filter_by(is_nifty50=True)
        elif index == "nifty100":
            query = query.filter_by(is_nifty100=True)
        elif index == "nifty500":
            query = query.filter_by(is_nifty500=True)
        
        if sector:
            query = query.filter(Stock.sector.ilike(f"%{sector}%"))
        
        stocks = query.all()
        
        return [
            {
                "symbol": s.symbol,
                "name": s.name,
                "sector": s.sector,
                "industry": s.industry
            }
            for s in stocks
        ]


def get_stock_details(symbol: str) -> Dict[str, Any]:
    """
    Get database details for a stock.
    
    Args:
        symbol: Stock symbol
    
    Returns:
        Stock details from database
    """
    with get_db_session() as db:
        stock = db.query(Stock).filter_by(symbol=symbol.upper()).first()
        
        if not stock:
            return {"error": f"Stock {symbol} not found in database"}
        
        return {
            "symbol": stock.symbol,
            "name": stock.name,
            "sector": stock.sector,
            "industry": stock.industry,
            "exchange": stock.exchange,
            "isin": stock.isin,
            "nse_token": stock.nse_token,
            "is_nifty50": stock.is_nifty50,
            "is_nifty100": stock.is_nifty100,
            "is_nifty500": stock.is_nifty500
        }


# ============================================
# MEMORY TOOLS
# ============================================

def read_memory() -> str:
    """
    Read the long-term trading memory.
    
    Returns:
        Memory file content
    """
    kb = _get_knowledge()
    return kb.get_memory() or "No memory file found"


def write_to_memory(section: str, content: str) -> bool:
    """
    Append to a section in the memory file.
    
    Args:
        section: Section name (e.g., "Trade Outcomes", "Market Patterns Observed")
        content: Text to append
    
    Returns:
        Success status
    """
    kb = _get_knowledge()
    return kb.append_to_memory(section, content)


def record_trade_outcome(symbol: str, outcome: str, notes: str) -> bool:
    """
    Record a trade outcome for learning.
    
    Args:
        symbol: Stock traded
        outcome: "Profit +X%" or "Loss -X%"
        notes: What happened, lessons learned
    
    Returns:
        Success status
    """
    kb = _get_knowledge()
    kb.record_trade_outcome(symbol, outcome, notes)
    return True


# ============================================
# NEWS TOOLS (placeholder - to be implemented)
# ============================================

def get_news(symbol: str, limit: int = 5) -> List[Dict[str, str]]:
    """
    Get recent news for a stock.
    
    Args:
        symbol: Stock symbol
        limit: Max news items
    
    Returns:
        List of news items with title, summary, date
    
    Note: Currently returns placeholder. Will integrate news API later.
    """
    # TODO: Integrate news API (MoneyControl, Economic Times, etc.)
    return [
        {
            "title": f"No live news available for {symbol}",
            "summary": "News integration pending. Check knowledge file for manual updates.",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "source": "placeholder"
        }
    ]


# ============================================
# AGGREGATED CONTEXT TOOLS
# ============================================

def get_full_context(symbol: str) -> Dict[str, Any]:
    """
    Get complete context for a stock (used by lead trader).
    Combines quote, technicals, knowledge, sector info.
    
    Args:
        symbol: Stock symbol
    
    Returns:
        Complete context dict
    """
    symbol = symbol.upper()
    
    context = {
        "symbol": symbol,
        "timestamp": datetime.now().isoformat()
    }
    
    # Quote
    context["quote"] = get_quote(symbol)
    
    # Technicals
    context["technicals"] = get_technicals(symbol)
    
    # Stock info
    context["stock_info"] = get_stock_info(symbol)
    
    # Get sector from DB and fetch sector info
    with get_db_session() as db:
        stock = db.query(Stock).filter_by(symbol=symbol).first()
        if stock and stock.sector:
            context["sector_info"] = get_sector_info(stock.sector)
    
    # News
    context["news"] = get_news(symbol)
    
    return context


# ============================================
# TOOL REGISTRY (for MCP integration)
# ============================================

AVAILABLE_TOOLS = {
    # Market Data
    "get_quote": {
        "function": get_quote,
        "description": "Get current price quote for a stock",
        "parameters": {"symbol": "Stock symbol (e.g., RELIANCE)"}
    },
    "get_historical": {
        "function": get_historical,
        "description": "Get historical OHLCV candle data",
        "parameters": {"symbol": "Stock symbol", "days": "Number of days (default 60)"}
    },
    "get_technicals": {
        "function": get_technicals,
        "description": "Get technical analysis (RSI, MACD, MAs, signals)",
        "parameters": {"symbol": "Stock symbol", "days": "Days to analyze (default 100)"}
    },
    
    # Knowledge
    "get_stock_info": {
        "function": get_stock_info,
        "description": "Get knowledge file content for a stock",
        "parameters": {"symbol": "Stock symbol"}
    },
    "get_sector_info": {
        "function": get_sector_info,
        "description": "Get sector overview and analysis",
        "parameters": {"sector": "Sector name (IT, Banking, Pharma, etc.)"}
    },
    "search_knowledge": {
        "function": search_knowledge,
        "description": "Search across all knowledge files",
        "parameters": {"query": "Search term", "limit": "Max results (default 5)"}
    },
    "get_strategy": {
        "function": get_strategy,
        "description": "Get trading strategy description",
        "parameters": {"strategy_name": "Strategy name (breakout, swing_trading, etc.)"}
    },
    
    # Stock Universe
    "list_stocks": {
        "function": list_stocks,
        "description": "List stocks from index or sector",
        "parameters": {"index": "nifty50/nifty100/nifty500", "sector": "Optional sector filter"}
    },
    "get_stock_details": {
        "function": get_stock_details,
        "description": "Get database details for a stock",
        "parameters": {"symbol": "Stock symbol"}
    },
    
    # Memory
    "read_memory": {
        "function": read_memory,
        "description": "Read long-term trading memory",
        "parameters": {}
    },
    "write_to_memory": {
        "function": write_to_memory,
        "description": "Append to memory file",
        "parameters": {"section": "Section name", "content": "Text to append"}
    },
    "record_trade_outcome": {
        "function": record_trade_outcome,
        "description": "Record trade result for learning",
        "parameters": {"symbol": "Stock", "outcome": "Profit/Loss", "notes": "Lessons"}
    },
    
    # News
    "get_news": {
        "function": get_news,
        "description": "Get recent news for stock (placeholder)",
        "parameters": {"symbol": "Stock symbol", "limit": "Max items"}
    },
    
    # Aggregated
    "get_full_context": {
        "function": get_full_context,
        "description": "Get complete context for a stock",
        "parameters": {"symbol": "Stock symbol"}
    }
}


def execute_tool(tool_name: str, **kwargs) -> Any:
    """Execute a tool by name with given arguments."""
    if tool_name not in AVAILABLE_TOOLS:
        return {"error": f"Unknown tool: {tool_name}"}
    
    try:
        return AVAILABLE_TOOLS[tool_name]["function"](**kwargs)
    except Exception as e:
        logger.error(f"Tool {tool_name} failed: {e}")
        return {"error": str(e)}


def list_available_tools() -> List[Dict[str, Any]]:
    """List all available tools with descriptions."""
    return [
        {
            "name": name,
            "description": info["description"],
            "parameters": info["parameters"]
        }
        for name, info in AVAILABLE_TOOLS.items()
    ]


# Test
if __name__ == "__main__":
    print("=== Available Tools ===")
    for tool in list_available_tools():
        print(f"  {tool['name']}: {tool['description']}")
    
    print("\n=== Testing get_quote ===")
    print(get_quote("RELIANCE"))
    
    print("\n=== Testing get_technicals ===")
    result = get_technicals("TCS")
    print(f"Signal: {result.get('overall_signal')}, Bullish: {result.get('bullish_signals')}")
    
    print("\n=== Testing get_stock_info ===")
    info = get_stock_info("INFY")
    print(f"Has knowledge: {info.get('knowledge_file_exists')}")
    
    print("\n=== Testing list_stocks ===")
    stocks = list_stocks("nifty50", sector="IT")
    print(f"IT stocks in Nifty50: {[s['symbol'] for s in stocks]}")
