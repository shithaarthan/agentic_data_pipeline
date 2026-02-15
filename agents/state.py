"""
LangGraph State Definitions
Defines the shared state for the multi-agent workflow.
"""

from typing import TypedDict, Annotated, List, Dict, Optional, Any
from datetime import datetime
import operator


class AgentMessage(TypedDict):
    """Message from an agent in the workflow."""
    role: str  # 'technical', 'fundamental', 'risk', 'macro', 'trader'
    content: str
    timestamp: str
    recommendation: Optional[str]  # 'BUY', 'SELL', 'HOLD', 'STRONG_BUY', 'STRONG_SELL'
    confidence: Optional[float]  # 0-100
    reasoning: Optional[str]


class TradeParameters(TypedDict, total=False):
    """Trade parameters determined by the trader agent."""
    entry_price: Optional[float]
    target_price: Optional[float]
    stop_loss: Optional[float]
    position_size: Optional[float]  # % of portfolio
    risk_reward_ratio: Optional[float]


class AgentState(TypedDict):
    """
    Shared state for the LangGraph workflow.
    
    This state is passed between all nodes in the graph.
    """
    # Input
    symbol: str
    
    # Market data (populated at start)
    ohlcv_data: Optional[Dict[str, Any]]  # Recent OHLCV
    quote_data: Optional[Dict[str, Any]]  # Current quote
    technical_indicators: Optional[Dict[str, Any]]  # Calculated indicators
    
    # Fundamental data
    fundamentals: Optional[Dict[str, Any]]
    sector_data: Optional[Dict[str, Any]]
    
    # Agent messages (accumulated through workflow)
    messages: Annotated[List[AgentMessage], operator.add]
    
    # Agent recommendations
    technical_signal: Optional[str]
    technical_confidence: Optional[float]
    
    fundamental_signal: Optional[str]
    fundamental_confidence: Optional[float]
    fundamental_score: Optional[float]  # X/10
    
    risk_assessment: Optional[str]
    risk_level: Optional[str]  # 'LOW', 'MEDIUM', 'HIGH'
    
    macro_context: Optional[str]
    macro_sentiment: Optional[str]  # 'BULLISH', 'BEARISH', 'NEUTRAL'
    
    # Final decision
    final_recommendation: Optional[str]
    final_confidence: Optional[float]
    trade_parameters: Optional[TradeParameters]
    
    # Workflow control
    iteration: int
    max_iterations: int
    requires_human_review: bool
    human_feedback: Optional[str]
    
    # Metadata
    workflow_start: str
    workflow_end: Optional[str]
    errors: Annotated[List[str], operator.add]


def create_initial_state(symbol: str) -> AgentState:
    """Create initial state for a new workflow."""
    return {
        'symbol': symbol,
        'ohlcv_data': None,
        'quote_data': None,
        'technical_indicators': None,
        'fundamentals': None,
        'sector_data': None,
        'messages': [],
        'technical_signal': None,
        'technical_confidence': None,
        'fundamental_signal': None,
        'fundamental_confidence': None,
        'fundamental_score': None,
        'risk_assessment': None,
        'risk_level': None,
        'macro_context': None,
        'macro_sentiment': None,
        'final_recommendation': None,
        'final_confidence': None,
        'trade_parameters': None,
        'iteration': 0,
        'max_iterations': 3,
        'requires_human_review': False,
        'human_feedback': None,
        'workflow_start': datetime.now().isoformat(),
        'workflow_end': None,
        'errors': []
    }