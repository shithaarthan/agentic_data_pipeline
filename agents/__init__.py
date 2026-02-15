"""Agents package initialization."""

from agents.multi_agent import MultiAgentTradingCrew, TradingAgent, AgentRole
from agents.tools import (
    AVAILABLE_TOOLS,
    execute_tool,
    list_available_tools,
    get_quote,
    get_historical,
    get_technicals,
    get_stock_info,
    get_sector_info,
    get_full_context
)

__all__ = [
    # Multi-agent system
    "MultiAgentTradingCrew",
    "TradingAgent",
    "AgentRole",
    
    # Tools
    "AVAILABLE_TOOLS",
    "execute_tool",
    "list_available_tools",
    "get_quote",
    "get_historical",
    "get_technicals",
    "get_stock_info",
    "get_sector_info",
    "get_full_context",
]
