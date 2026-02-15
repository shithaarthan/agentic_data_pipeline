"""
Multi-Agent Trading System
Implements specialized agents that collaborate on trading decisions.
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from loguru import logger

from llm import LLMManager, ChatMessage


class AgentRole(Enum):
    """Roles for trading agents."""
    TECHNICAL_ANALYST = "technical_analyst"
    FUNDAMENTAL_ANALYST = "fundamental_analyst"
    SENTIMENT_ANALYST = "sentiment_analyst"
    RISK_MANAGER = "risk_manager"
    BULL_RESEARCHER = "bull_researcher"
    BEAR_RESEARCHER = "bear_researcher"
    TRADER = "trader"


@dataclass
class AgentMessage:
    """Message from an agent."""
    agent: str
    role: AgentRole
    content: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class AgentConfig:
    """Configuration for an agent."""
    name: str
    role: AgentRole
    system_prompt: str
    goal: str


# Agent System Prompts
AGENT_PROMPTS = {
    AgentRole.TECHNICAL_ANALYST: """You are a Technical Analyst specializing in Indian equity markets.

Your expertise:
- Chart patterns (Head & Shoulders, Double Top/Bottom, Triangles)
- Technical indicators (RSI, MACD, Moving Averages, Bollinger Bands)
- Support and resistance levels
- Volume analysis
- Trend identification

Your task: Analyze the provided technical data and give a clear technical perspective.
Focus only on price action and indicators. Be specific about entry/exit levels.
Format: Start with your signal (BULLISH/BEARISH/NEUTRAL) then explain.""",

    AgentRole.FUNDAMENTAL_ANALYST: """You are a Fundamental Analyst specializing in Indian equities.

Your expertise:
- Financial statement analysis
- Valuation metrics (P/E, P/B, EV/EBITDA)
- Industry analysis
- Management quality assessment
- Competitive positioning

Your task: Analyze the fundamental aspects of the company.
Consider valuation, growth prospects, and sector trends.
Format: Start with your assessment (UNDERVALUED/FAIRLY_VALUED/OVERVALUED) then explain.""",

    AgentRole.SENTIMENT_ANALYST: """You are a Sentiment Analyst for Indian markets.

Your expertise:
- News sentiment analysis
- Social media trends
- Institutional activity
- FII/DII flows
- Market mood indicators

Your task: Assess the current market sentiment around the stock.
Consider recent news, analyst opinions, and market psychology.
Format: Start with sentiment (POSITIVE/NEGATIVE/MIXED) then explain.""",

    AgentRole.RISK_MANAGER: """You are a Risk Manager for a trading desk.

Your expertise:
- Position sizing
- Stop-loss strategies
- Portfolio risk assessment
- Volatility analysis
- Drawdown management

Your task: Evaluate the risk aspects of the proposed trade.
Define appropriate position size, stop-loss, and maximum risk.
Format: Start with risk level (LOW/MEDIUM/HIGH) then provide recommendations.""",

    AgentRole.BULL_RESEARCHER: """You are a Bull Researcher - your job is to make the bullish case.

Your task: Present the strongest possible bullish argument for this stock.
- Identify potential catalysts for upside
- Highlight positive factors
- Counter bearish arguments
- Set realistic upside targets

Be thorough but honest. Don't ignore real risks.
Format: Present your bullish thesis clearly.""",

    AgentRole.BEAR_RESEARCHER: """You are a Bear Researcher - your job is to make the bearish case.

Your task: Present the strongest possible bearish argument for this stock.
- Identify potential risks and red flags
- Highlight negative factors
- Counter bullish arguments
- Set realistic downside targets

Be thorough but honest. Acknowledge genuine strengths.
Format: Present your bearish thesis clearly.""",

    AgentRole.TRADER: """You are the Lead Trader making the final decision.

You have received input from:
- Technical Analyst
- Sentiment Analyst  
- Bull Researcher (bullish case)
- Bear Researcher (bearish case)
- Risk Manager

Your task: Synthesize all inputs and make a final trading decision.
Weigh the different perspectives and make a clear recommendation.

Required output format:
DECISION: [BUY/SELL/HOLD]
CONFIDENCE: [0-100]%
ENTRY: ₹[price]
TARGET: ₹[price]
STOP_LOSS: ₹[price]
POSITION_SIZE: [% of portfolio]

RATIONALE: [Explain your decision considering all inputs]"""
}


class TradingAgent:
    """Individual trading agent with a specific role."""
    
    def __init__(self, config: AgentConfig, llm: LLMManager):
        self.config = config
        self.llm = llm
        self.messages_sent: List[AgentMessage] = []
    
    def analyze(self, context: str) -> AgentMessage:
        """
        Perform analysis based on agent's role.
        
        Args:
            context: The context/data to analyze
        
        Returns:
            AgentMessage with the analysis
        """
        messages = [
            ChatMessage(role="system", content=self.config.system_prompt),
            ChatMessage(role="user", content=f"Goal: {self.config.goal}\n\n{context}")
        ]
        
        response = self.llm.chat(messages)
        
        message = AgentMessage(
            agent=self.config.name,
            role=self.config.role,
            content=response.content
        )
        
        self.messages_sent.append(message)
        return message


class MultiAgentTradingCrew:
    """
    Multi-agent trading crew that collaborates on analysis.
    Implements a simplified CrewAI-like pattern.
    """
    
    def __init__(self):
        self.llm = LLMManager()
        self.agents: Dict[AgentRole, TradingAgent] = {}
        self.discussion_history: List[AgentMessage] = []
        
        self._initialize_agents()
    
    def _initialize_agents(self):
        """Initialize all agents."""
        agent_configs = [
            AgentConfig(
                name="Alex (Technical)",
                role=AgentRole.TECHNICAL_ANALYST,
                system_prompt=AGENT_PROMPTS[AgentRole.TECHNICAL_ANALYST],
                goal="Provide technical analysis perspective"
            ),
            AgentConfig(
                name="Sam (Sentiment)",
                role=AgentRole.SENTIMENT_ANALYST,
                system_prompt=AGENT_PROMPTS[AgentRole.SENTIMENT_ANALYST],
                goal="Assess market sentiment"
            ),
            AgentConfig(
                name="Blake (Bull)",
                role=AgentRole.BULL_RESEARCHER,
                system_prompt=AGENT_PROMPTS[AgentRole.BULL_RESEARCHER],
                goal="Present the bullish case"
            ),
            AgentConfig(
                name="Bailey (Bear)",
                role=AgentRole.BEAR_RESEARCHER,
                system_prompt=AGENT_PROMPTS[AgentRole.BEAR_RESEARCHER],
                goal="Present the bearish case"
            ),
            AgentConfig(
                name="Riley (Risk)",
                role=AgentRole.RISK_MANAGER,
                system_prompt=AGENT_PROMPTS[AgentRole.RISK_MANAGER],
                goal="Evaluate risk and position sizing"
            ),
            AgentConfig(
                name="Jordan (Trader)",
                role=AgentRole.TRADER,
                system_prompt=AGENT_PROMPTS[AgentRole.TRADER],
                goal="Make final trading decision"
            ),
        ]
        
        for config in agent_configs:
            self.agents[config.role] = TradingAgent(config, self.llm)
        
        logger.info(f"Initialized {len(self.agents)} trading agents")
    
    def analyze_stock(
        self, 
        symbol: str,
        additional_context: str = ""
    ) -> Dict[str, Any]:
        """
        Run full multi-agent analysis using MCP tools.
        
        Args:
            symbol: Stock symbol
            additional_context: Any extra context to include
        
        Returns:
            Dictionary with all agent outputs and final decision
        """
        from agents.tools import get_quote, get_technicals, get_stock_info, get_sector_info, get_news
        
        self.discussion_history = []
        results = {"symbol": symbol, "agents": {}}
        
        # Fetch data using MCP tools
        logger.info(f"Fetching data for {symbol} using MCP tools...")
        
        quote_data = get_quote(symbol)
        tech_data = get_technicals(symbol)
        stock_info = get_stock_info(symbol)
        news_data = get_news(symbol)
        
        # Get sector info if available
        sector_info = {}
        if stock_info.get("knowledge_file_exists") or stock_info.get("sector"):
            sector = stock_info.get("sector") or "Unknown"
            sector_info = get_sector_info(sector)
        
        # Build context from tools output
        quote_text = f"""
Current Quote:
  LTP: ₹{quote_data.get('ltp', 'N/A'):,.2f}
  Change: {quote_data.get('change', 0):+.2f} ({quote_data.get('change_pct', 0):+.2f}%)
  High: ₹{quote_data.get('high', 'N/A'):,.2f}
  Low: ₹{quote_data.get('low', 'N/A'):,.2f}
  Volume: {quote_data.get('volume', 'N/A'):,}
"""
        
        tech_text = f"""
Technical Analysis:
  Overall Signal: {tech_data.get('overall_signal', 'N/A')}
  Bullish Signals: {tech_data.get('bullish_signals', 0)}
  Bearish Signals: {tech_data.get('bearish_signals', 0)}
  
  {tech_data.get('analysis_text', '')}
"""
        
        knowledge_text = ""
        if stock_info.get("knowledge"):
            knowledge_text = f"\nStock Knowledge:\n{stock_info.get('knowledge', '')[:1500]}"
        
        sector_text = ""
        if sector_info.get("knowledge"):
            sector_text = f"\nSector Context:\n{sector_info.get('knowledge', '')[:800]}"
        
        # Prepare base context
        base_context = f"""
Stock: {symbol}
{quote_text}
{tech_text}
{knowledge_text}
{sector_text}
"""
        if additional_context:
            base_context += f"\nAdditional Context:\n{additional_context}"
        
        # Phase 1: Parallel Analysis (in sequence for simplicity)
        logger.info(f"Phase 1: Initial analysis for {symbol}")
        
        # Technical Analysis
        tech_msg = self.agents[AgentRole.TECHNICAL_ANALYST].analyze(base_context)
        self.discussion_history.append(tech_msg)
        results["agents"]["technical"] = tech_msg.content
        logger.debug(f"Technical: {tech_msg.content[:100]}...")
        
        # Sentiment Analysis
        sent_msg = self.agents[AgentRole.SENTIMENT_ANALYST].analyze(base_context)
        self.discussion_history.append(sent_msg)
        results["agents"]["sentiment"] = sent_msg.content
        logger.debug(f"Sentiment: {sent_msg.content[:100]}...")
        
        # Phase 2: Bull vs Bear Research
        logger.info("Phase 2: Bull vs Bear debate")
        
        debate_context = f"""{base_context}

Technical Analyst View:
{tech_msg.content}

Sentiment Analysis:
{sent_msg.content}
"""
        
        bull_msg = self.agents[AgentRole.BULL_RESEARCHER].analyze(debate_context)
        self.discussion_history.append(bull_msg)
        results["agents"]["bull_case"] = bull_msg.content
        
        bear_msg = self.agents[AgentRole.BEAR_RESEARCHER].analyze(debate_context)
        self.discussion_history.append(bear_msg)
        results["agents"]["bear_case"] = bear_msg.content
        
        # Phase 3: Risk Assessment
        logger.info("Phase 3: Risk assessment")
        
        risk_context = f"""{debate_context}

Bull Case:
{bull_msg.content}

Bear Case:
{bear_msg.content}
"""
        
        risk_msg = self.agents[AgentRole.RISK_MANAGER].analyze(risk_context)
        self.discussion_history.append(risk_msg)
        results["agents"]["risk"] = risk_msg.content
        
        # Phase 4: Final Decision
        logger.info("Phase 4: Final trading decision")
        
        decision_context = f"""
Stock: {symbol}

{quote_data}

=== TEAM ANALYSIS ===

TECHNICAL ANALYST:
{tech_msg.content}

SENTIMENT ANALYST:
{sent_msg.content}

BULL RESEARCHER:
{bull_msg.content}

BEAR RESEARCHER:
{bear_msg.content}

RISK MANAGER:
{risk_msg.content}

=== END TEAM ANALYSIS ===

Based on all the above inputs, make your final trading decision.
"""
        
        trader_msg = self.agents[AgentRole.TRADER].analyze(decision_context)
        self.discussion_history.append(trader_msg)
        results["agents"]["trader_decision"] = trader_msg.content
        
        # Parse final decision
        results["final_decision"] = self._parse_trader_decision(trader_msg.content)
        
        logger.success(f"Multi-agent analysis complete for {symbol}")
        return results
    
    def _parse_trader_decision(self, text: str) -> Dict[str, Any]:
        """Parse the trader's final decision."""
        decision = {
            "signal": "HOLD",
            "confidence": 50,
            "entry": None,
            "target": None,
            "stop_loss": None,
            "position_size": None,
            "rationale": ""
        }
        
        lines = text.split('\n')
        rationale_start = False
        rationale_lines = []
        
        for line in lines:
            line_upper = line.upper().strip()
            
            if line_upper.startswith("DECISION:"):
                val = line_upper.replace("DECISION:", "").strip()
                if "BUY" in val:
                    decision["signal"] = "BUY"
                elif "SELL" in val:
                    decision["signal"] = "SELL"
            
            elif line_upper.startswith("CONFIDENCE:"):
                try:
                    val = line_upper.replace("CONFIDENCE:", "").replace("%", "").strip()
                    decision["confidence"] = float(val)
                except:
                    pass
            
            elif line_upper.startswith("ENTRY:"):
                try:
                    val = line.split(":")[1].replace("₹", "").replace(",", "").strip()
                    decision["entry"] = float(val)
                except:
                    pass
            
            elif line_upper.startswith("TARGET:"):
                try:
                    val = line.split(":")[1].replace("₹", "").replace(",", "").strip()
                    decision["target"] = float(val)
                except:
                    pass
            
            elif line_upper.startswith("STOP_LOSS:"):
                try:
                    val = line.split(":")[1].replace("₹", "").replace(",", "").strip()
                    decision["stop_loss"] = float(val)
                except:
                    pass
            
            elif line_upper.startswith("POSITION_SIZE:"):
                try:
                    val = line.split(":")[1].replace("%", "").strip()
                    decision["position_size"] = float(val)
                except:
                    pass
            
            elif line_upper.startswith("RATIONALE:"):
                rationale_start = True
                rationale_lines.append(line.split(":", 1)[1].strip() if ":" in line else "")
            
            elif rationale_start:
                rationale_lines.append(line)
        
        decision["rationale"] = "\n".join(rationale_lines).strip()
        
        return decision
    
    def get_discussion_transcript(self) -> str:
        """Get full transcript of agent discussion."""
        lines = ["=== Multi-Agent Trading Discussion ===\n"]
        
        for msg in self.discussion_history:
            lines.append(f"--- {msg.agent} ({msg.role.value}) ---")
            lines.append(msg.content)
            lines.append("")
        
        return "\n".join(lines)


# Usage example
if __name__ == "__main__":
    print("=== Testing Multi-Agent Trading System ===")
    
    symbol = "TCS"
    
    # Run multi-agent analysis (tools fetch data internally)
    crew = MultiAgentTradingCrew()
    results = crew.analyze_stock(symbol)
    
    print("\n=== Final Decision ===")
    fd = results["final_decision"]
    print(f"Signal: {fd['signal']} (Confidence: {fd['confidence']}%)")
    if fd["entry"]:
        print(f"Entry: ₹{fd['entry']:,.2f}")
    if fd["target"]:
        print(f"Target: ₹{fd['target']:,.2f}")
    if fd["stop_loss"]:
        print(f"Stop Loss: ₹{fd['stop_loss']:,.2f}")
    print(f"\nRationale: {fd['rationale'][:300]}...")
    
    # Show transcript
    print("\n" + "="*50)
    print(crew.get_discussion_transcript()[:2000])
