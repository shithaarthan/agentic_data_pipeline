"""
FastAPI REST API for Foxa Trading Platform
Provides endpoints for signals, stocks, and agent analysis.
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import date, datetime
from contextlib import asynccontextmanager

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from loguru import logger

from lakehouse.iceberg_catalog import get_catalog
from lakehouse.gold import GoldAnalytics
from agents.langgraph_workflow import TradingAgentWorkflow
from data.market_data import MarketData


# ============================================================
# Pydantic Models
# ============================================================

class SignalResponse(BaseModel):
    symbol: str
    date: date
    strategy: str
    signal: str
    entry: Optional[float]
    target: Optional[float]
    stop: Optional[float]
    confidence: Optional[str]
    rsi: Optional[float]
    adx: Optional[float]


class AnalysisResponse(BaseModel):
    symbol: str
    timestamp: str
    final_recommendation: str
    final_confidence: float
    technical_signal: Optional[str]
    fundamental_signal: Optional[str]
    risk_level: Optional[str]
    trade_parameters: Optional[Dict[str, Any]]


class QuoteResponse(BaseModel):
    symbol: str
    ltp: float
    change: float
    change_pct: float
    volume: int
    open: float
    high: float
    low: float


# ============================================================
# FastAPI App
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("🚀 Starting Foxa API")
    yield
    logger.info("🛑 Shutting down Foxa API")


app = FastAPI(
    title="Foxa Trading API",
    description="REST API for Foxa Trading Platform - Data Engineering Showcase",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
_gold_analytics = None
_workflow = None
_market_data = None


def get_gold_analytics():
    global _gold_analytics
    if _gold_analytics is None:
        _gold_analytics = GoldAnalytics()
    return _gold_analytics


def get_workflow():
    global _workflow
    if _workflow is None:
        _workflow = TradingAgentWorkflow()
    return _workflow


def get_market_data():
    global _market_data
    if _market_data is None:
        _market_data = MarketData()
    return _market_data


# ============================================================
# Endpoints
# ============================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }


@app.get("/signals", response_model=List[SignalResponse])
async def get_signals(
    min_confidence: str = Query("MEDIUM"),
    limit: int = Query(50, ge=1, le=100)
):
    """Get latest trading signals."""
    try:
        gold = get_gold_analytics()
        df = gold.get_latest_signals(min_confidence)
        
        if df.empty:
            return []
        
        df = df.head(limit)
        signals = []
        for _, row in df.iterrows():
            signals.append(SignalResponse(
                symbol=row.get('symbol', ''),
                date=row.get('date', date.today()),
                strategy=row.get('strategy', ''),
                signal=row.get('signal', ''),
                entry=row.get('entry'),
                target=row.get('target'),
                stop=row.get('stop'),
                confidence=row.get('confidence'),
                rsi=row.get('rsi'),
                adx=row.get('adx')
            ))
        return signals
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stocks/{symbol}/quote", response_model=QuoteResponse)
async def get_stock_quote(symbol: str):
    """Get current quote for a stock."""
    try:
        md = get_market_data()
        quote = md.get_quote(symbol.upper())
        
        if quote is None:
            raise HTTPException(status_code=404, detail=f"Quote for {symbol} not found")
        
        return QuoteResponse(
            symbol=quote.symbol,
            ltp=quote.ltp,
            change=quote.change,
            change_pct=quote.change_pct,
            volume=quote.volume,
            open=quote.open,
            high=quote.high,
            low=quote.low
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/analyze", response_model=AnalysisResponse)
async def analyze_stock(symbol: str):
    """Run LangGraph multi-agent analysis on a stock."""
    try:
        workflow = get_workflow()
        result = workflow.analyze(symbol.upper())
        
        return AnalysisResponse(
            symbol=result['symbol'],
            timestamp=result['timestamp'],
            final_recommendation=result['final_recommendation'],
            final_confidence=result['final_confidence'],
            technical_signal=result.get('technical_signal'),
            fundamental_signal=result.get('fundamental_signal'),
            risk_level=result.get('risk_level'),
            trade_parameters=result.get('trade_parameters')
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
async def root():
    """API root."""
    return {
        "name": "Foxa Trading API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": [
            "/health",
            "/signals",
            "/stocks/{symbol}/quote",
            "/analyze"
        ]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)