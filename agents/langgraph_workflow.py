"""
LangGraph Multi-Agent Workflow
State machine orchestration for trading analysis.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, TypedDict
from datetime import datetime
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

# LangGraph imports
try:
    from langgraph.graph import StateGraph, END
except ImportError:
    logger.error("langgraph not installed. Run: pip install langgraph")
    raise

from agents.state import AgentState, create_initial_state
from agents.nodes.data_loader import (
    data_loader_node,
    technical_analyst_node,
    fundamental_analyst_node,
    risk_manager_node,
    macro_analyst_node,
    trader_node
)


class TradingAgentWorkflow:
    """
    LangGraph-based multi-agent trading analysis workflow.
    
    Graph Structure:
    
                    ┌─────────────────┐
                    │   Data Loader   │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │  Technical  │ │ Fundamental │ │    Macro    │
    │   Analyst   │ │   Analyst   │ │   Analyst   │
    └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
           │               │               │
           └───────────────┼───────────────┘
                           ▼
                  ┌─────────────────┐
                  │   Risk Manager  │
                  └────────┬────────┘
                           ▼
                  ┌─────────────────┐
                  │     Trader      │
                  │  (Final Decision)│
                  └─────────────────┘
    """
    
    def __init__(self):
        self.graph = None
        self._build_graph()
    
    def _build_graph(self):
        """Build the LangGraph state machine."""
        
        # Create graph
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("data_loader", data_loader_node)
        workflow.add_node("technical_analyst", technical_analyst_node)
        workflow.add_node("fundamental_analyst", fundamental_analyst_node)
        workflow.add_node("macro_analyst", macro_analyst_node)
        workflow.add_node("risk_manager", risk_manager_node)
        workflow.add_node("trader", trader_node)
        
        # Add edges from data loader to all analysts (parallel)
        workflow.add_edge("data_loader", "technical_analyst")
        workflow.add_edge("data_loader", "fundamental_analyst")
        workflow.add_edge("data_loader", "macro_analyst")
        
        # Add conditional edges from analysts to risk manager
        # Wait for all analysts to complete before risk manager
        workflow.add_edge("technical_analyst", "risk_manager")
        workflow.add_edge("fundamental_analyst", "risk_manager")
        workflow.add_edge("macro_analyst", "risk_manager")
        
        # Risk manager to trader
        workflow.add_edge("risk_manager", "trader")
        
        # Trader to end
        workflow.add_edge("trader", END)
        
        # Set entry point
        workflow.set_entry_point("data_loader")
        
        # Compile graph
        self.graph = workflow.compile()
        
        logger.info("LangGraph workflow compiled successfully")
    
    def analyze(self, symbol: str) -> Dict[str, Any]:
        """
        Run the complete analysis workflow for a symbol.
        
        Args:
            symbol: Stock symbol to analyze
            
        Returns:
            Analysis results including final recommendation
        """
        logger.info(f"🚀 Starting LangGraph analysis for {symbol}")
        
        # Create initial state
        initial_state = create_initial_state(symbol)
        
        # Run graph
        try:
            final_state = self.graph.invoke(initial_state)
            
            # Extract results
            result = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'final_recommendation': final_state.get('final_recommendation'),
                'final_confidence': final_state.get('final_confidence'),
                'trade_parameters': final_state.get('trade_parameters'),
                'technical_signal': final_state.get('technical_signal'),
                'fundamental_signal': final_state.get('fundamental_signal'),
                'fundamental_score': final_state.get('fundamental_score'),
                'risk_level': final_state.get('risk_level'),
                'macro_sentiment': final_state.get('macro_sentiment'),
                'messages': final_state.get('messages', []),
                'workflow_start': final_state.get('workflow_start'),
                'workflow_end': final_state.get('workflow_end')
            }
            
            logger.success(
                f"✅ Analysis complete: {result['final_recommendation']} "
                f"({result['final_confidence']:.0f}%)"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            return {
                'symbol': symbol,
                'error': str(e),
                'final_recommendation': 'ERROR',
                'final_confidence': 0
            }
    
    def get_discussion_transcript(self, result: Dict[str, Any]) -> str:
        """
        Format the agent discussion as a transcript.
        
        Args:
            result: Analysis result from analyze()
            
        Returns:
            Formatted transcript string
        """
        lines = [
            "=" * 70,
            f"🤖 MULTI-AGENT ANALYSIS: {result['symbol']}",
            f"📅 {result.get('timestamp', 'N/A')}",
            "=" * 70,
            ""
        ]
        
        messages = result.get('messages', [])
        
        for msg in messages:
            role = msg.get('role', 'unknown').upper()
            content = msg.get('content', '')
            reasoning = msg.get('reasoning', '')
            
            lines.append(f"[{role}]")
            lines.append(f"  {content}")
            if reasoning:
                lines.append(f"  Reasoning: {reasoning}")
            lines.append("")
        
        lines.extend([
            "=" * 70,
            "📊 FINAL DECISION",
            "=" * 70,
            f"Signal: {result.get('final_recommendation', 'N/A')}",
            f"Confidence: {result.get('final_confidence', 0):.0f}%",
            ""
        ])
        
        params = result.get('trade_parameters', {})
        if params:
            lines.append("Trade Parameters:")
            if params.get('entry_price'):
                lines.append(f"  Entry: ₹{params['entry_price']:.2f}")
            if params.get('target_price'):
                lines.append(f"  Target: ₹{params['target_price']:.2f}")
            if params.get('stop_loss'):
                lines.append(f"  Stop: ₹{params['stop_loss']:.2f}")
            if params.get('risk_reward_ratio'):
                lines.append(f"  R:R: {params['risk_reward_ratio']:.1f}")
        
        lines.append("=" * 70)
        
        return "\n".join(lines)
    
    def visualize_graph(self, output_path: str = "agent_graph.png"):
        """
        Generate a visualization of the agent graph.
        Requires graphviz.
        """
        try:
            # Get graph visualization
            from IPython.display import Image, display
            
            # Note: This would work in a Jupyter notebook
            # For CLI, we'd need to save differently
            logger.info("Graph visualization available in notebook environment")
            
        except Exception as e:
            logger.warning(f"Cannot visualize graph: {e}")


# ============================================================
# CLI Interface
# ============================================================

if __name__ == "__main__":
    import argparse
    import json
    
    parser = argparse.ArgumentParser(
        description="LangGraph Multi-Agent Trading Analysis"
    )
    parser.add_argument("symbol", type=str, help="Stock symbol to analyze")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--transcript", action="store_true", help="Show full transcript")
    
    args = parser.parse_args()
    
    # Initialize and run
    workflow = TradingAgentWorkflow()
    result = workflow.analyze(args.symbol.upper())
    
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    elif args.transcript:
        print(workflow.get_discussion_transcript(result))
    else:
        print("\n" + "=" * 60)
        print(f"ANALYSIS RESULT: {result['symbol']}")
        print("=" * 60)
        print(f"Final Signal: {result['final_recommendation']}")
        print(f"Confidence: {result['final_confidence']:.0f}%")
        print(f"Technical: {result.get('technical_signal', 'N/A')}")
        print(f"Fundamental: {result.get('fundamental_signal', 'N/A')}")
        print(f"Risk Level: {result.get('risk_level', 'N/A')}")
        
        params = result.get('trade_parameters', {})
        if params.get('entry_price'):
            print(f"\nEntry: ₹{params['entry_price']:.2f}")
            print(f"Target: ₹{params['target_price']:.2f}")
            print(f"Stop: ₹{params['stop_loss']:.2f}")