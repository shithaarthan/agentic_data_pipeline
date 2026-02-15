"""
Agent Pipeline Orchestrator
Reads signals, runs agents, updates knowledge files.
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.multi_agent import MultiAgentTradingCrew, AgentRole
from agents.fundamental_agent import FundamentalAgent
from data.knowledge import KnowledgeReader

# Paths
SIGNALS_DIR = Path(__file__).parent.parent / "data" / "signals"


class AgentPipeline:
    """
    Orchestrates the multi-agent analysis flow.
    Read Signal -> Agent Analysis -> Update Knowledge File
    """
    
    def __init__(self):
        self.crew = MultiAgentTradingCrew()
        self.fundamental_agent = FundamentalAgent()
        self.kb = KnowledgeReader()
        
    def run_technical_analysis(self, symbols: list):
        """Run Technical Analyst agent on symbols."""
        agent = self.crew.agents[AgentRole.TECHNICAL_ANALYST]
        
        for symbol in symbols:
            logger.info(f"Running Technical Agent for {symbol}...")
            
            # Get context from MCP tools
            from agents.tools import get_technicals, get_quote
            
            quotes = get_quote(symbol)
            techs = get_technicals(symbol)
            
            full_prompt = f"""
STRICT INSTRUCTION: You are the Technical Analyst.
Analyze {symbol} based on the following data:

CURRENT MARKET DATA:
LTP: {quotes.get('ltp')}
Change: {quotes.get('change_pct')}%

TECHNICAL INDICATORS:
{techs.get('analysis_text', 'N/A')}

Provide a concise technical summary (bullet points) for the knowledge base.
Focus on: Trend, Support/Resistance, Volume, and entry/exit levels.
"""
            
            response = agent.analyze(full_prompt)
            content = response.content
            
            # Update Knowledge File
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            section_content = f"*Updated: {timestamp}*\n\n{content}"
            
            if self.kb.update_section(symbol, "Agent: Technical Analysis", section_content):
                logger.success(f"Updated {symbol}.md with Technical Analysis")
            else:
                logger.error(f"Failed to update {symbol}.md")
    
    def run_fundamental_analysis(self, symbols: list):
        """Run Fundamental Agent on symbols."""
        for symbol in symbols:
            self.fundamental_agent.update_knowledge_file(symbol)
    
    def run_full_pipeline(self, signal_file: str = "kimi_2026-01-30.json"):
        """
        Run complete agent pipeline on signals.
        """
        file_path = SIGNALS_DIR / signal_file
        if not file_path.exists():
            logger.error(f"Signal file not found: {file_path}")
            return
            
        with open(file_path) as f:
            data = json.load(f)
            
        signals = data.get("signals", [])
        symbols = list(set(s["symbol"] for s in signals))
        
        logger.info(f"Processing {len(symbols)} stocks through agent pipeline...")
        
        # Run agents
        logger.info("\n=== PHASE 1: Technical Analysis ===")
        self.run_technical_analysis(symbols)
        
        logger.info("\n=== PHASE 2: Fundamental Analysis ===")
        self.run_fundamental_analysis(symbols)
        
        logger.success(f"\n✅ Pipeline complete! Updated {len(symbols)} knowledge files.")
        logger.info(f"Next: Review knowledge files in knowledge/stocks/")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Agent Pipeline")
    parser.add_argument("--signals", type=str, default="kimi_2026-01-30.json", help="Signal file to process")
    parser.add_argument("--technical-only", action="store_true", help="Run only technical analysis")
    parser.add_argument("--fundamental-only", action="store_true", help="Run only fundamental analysis")
    
    args = parser.parse_args()
    
    pipeline = AgentPipeline()
    
    if args.technical_only:
        file_path = SIGNALS_DIR / args.signals
        with open(file_path) as f:
            data = json.load(f)
        symbols = list(set(s["symbol"] for s in data["signals"]))
        pipeline.run_technical_analysis(symbols)
    elif args.fundamental_only:
        file_path = SIGNALS_DIR / args.signals
        with open(file_path) as f:
            data = json.load(f)
        symbols = list(set(s["symbol"] for s in data["signals"]))
        pipeline.run_fundamental_analysis(symbols)
    else:
        pipeline.run_full_pipeline(args.signals)
