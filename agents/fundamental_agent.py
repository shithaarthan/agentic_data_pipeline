"""
Fundamental Analysis Agent
Fetches comprehensive fundamental data and performs LLM-based analysis.
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, Optional
import httpx
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from data.knowledge import KnowledgeReader
from data.fundamental_fetcher import FundamentalDataFetcher


class FundamentalAgent:
    """
    Fundamental Analysis Agent using Qwen3 via OpenRouter.
    Fetches comprehensive data from Screener.in and FMP, then analyzes with LLM.
    """
    
    def __init__(self):
        self.kb = KnowledgeReader()
        self.fetcher = FundamentalDataFetcher()
        self.api_key = settings.openrouter_api_key
        # Use provided model name, or fallback to known working free model
        self.model = settings.openrouter_model or "qwen/qwen-2.5-72b-instruct:free"
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        
        if not self.api_key:
            logger.warning("OpenRouter API key not configured")
    
    def _call_llm(self, prompt: str) -> str:
        """Call OpenRouter API with Qwen model."""
        if not self.api_key:
            return "Error: OpenRouter API key not configured"
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/foxa-trading",
            "X-Title": "Foxa Trading Assistant"
        }
        
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a fundamental analysis expert for Indian stock markets. Provide concise, actionable insights based on comprehensive financial data."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.3,
            "max_tokens": 1500
        }
        
        try:
            with httpx.Client(timeout=60.0) as client:
                logger.debug(f"Sending request to OpenRouter model: {self.model}")
                response = client.post(self.base_url, json=payload, headers=headers)
                
                if response.status_code != 200:
                    logger.error(f"LLM API Error: {response.status_code} - {response.text}")
                    return f"Error calling LLM ({response.status_code}): {response.text}"
                
                data = response.json()
                if "choices" in data and len(data["choices"]) > 0:
                    return data["choices"][0]["message"]["content"]
                elif "error" in data:
                    return f"OpenRouter Error: {data['error']['message']}"
                else:
                    return "Error: Unexpected response format from OpenRouter"
                    
        except Exception as e:
            logger.error(f"LLM API Exception: {e}")
            return f"Error calling LLM: {str(e)}"
    
    def _build_analysis_prompt(self, symbol: str, data: Dict) -> str:
        """Build comprehensive analysis prompt from fetched data."""
        
        prompt_parts = [f"Analyze the fundamental health of {symbol} for a 2-4 month position trade.\n"]
        
        # Screener.in data
        if data.get('screener'):
            screener = data['screener']
            
            prompt_parts.append("\n=== COMPANY OVERVIEW ===")
            if screener.get('company_name'):
                prompt_parts.append(f"Company: {screener['company_name']}")
            
            if screener.get('top_ratios'):
                prompt_parts.append("\nKEY METRICS:")
                for key, value in screener['top_ratios'].items():
                    prompt_parts.append(f"- {key}: {value}")
            
            if screener.get('quarterly_results'):
                prompt_parts.append("\n=== QUARTERLY PERFORMANCE (Last 4 Quarters) ===")
                for i, quarter in enumerate(screener['quarterly_results'][:4], 1):
                    prompt_parts.append(f"\nQ{i}:")
                    for key, value in quarter.items():
                        if key and value:
                            prompt_parts.append(f"  {key}: {value}")
            
            if screener.get('peers'):
                prompt_parts.append("\n=== PEER COMPARISON ===")
                for peer in screener['peers'][:5]:
                    prompt_parts.append(f"- {peer.get('name')}: Market Cap {peer.get('market_cap')}")
        
        # FMP data
        if data.get('fmp'):
            fmp = data['fmp']
            
            if fmp.get('profile'):
                profile = fmp['profile']
                prompt_parts.append("\n=== COMPANY PROFILE (FMP) ===")
                if profile.get('description'):
                    prompt_parts.append(f"Business: {profile['description'][:300]}...")
                if profile.get('sector'):
                    prompt_parts.append(f"Sector: {profile['sector']}")
                if profile.get('industry'):
                    prompt_parts.append(f"Industry: {profile['industry']}")
            
            if fmp.get('ratios'):
                ratios = fmp['ratios']
                prompt_parts.append("\n=== FINANCIAL RATIOS ===")
                key_ratios = ['returnOnEquity', 'returnOnAssets', 'debtEquityRatio', 
                             'currentRatio', 'quickRatio', 'operatingProfitMargin']
                for ratio in key_ratios:
                    if ratio in ratios:
                        prompt_parts.append(f"- {ratio}: {ratios[ratio]}")
            
            if fmp.get('growth'):
                growth = fmp['growth']
                prompt_parts.append("\n=== GROWTH METRICS ===")
                key_growth = ['revenueGrowth', 'netIncomeGrowth', 'epsgrowth']
                for metric in key_growth:
                    if metric in growth:
                        prompt_parts.append(f"- {metric}: {growth[metric]}")
        
        # Fallback
        if not data.get('screener') and not data.get('fmp'):
            metrics = self.get_fundamentals_from_knowledge(symbol)
            if metrics:
                prompt_parts.append("\n=== BASIC METRICS (from knowledge file) ===")
                for key, value in metrics.items():
                    prompt_parts.append(f"- {key}: {value}")
        
        # Analysis instructions
        prompt_parts.append("""

Based on the above data, provide a concise fundamental analysis in this format:

**Fundamental Score: X/10**

**Strengths:**
- [Key strength 1]
- [Key strength 2]
- [Key strength 3]

**Concerns:**
- [Key concern 1]
- [Key concern 2]

**Quarterly Trend:**
- [Is revenue/profit growing or declining? Any red flags?]

**Valuation:**
- [Fairly valued / Overvalued / Undervalued - with reasoning]

**Peer Position:**
- [How does it compare to peers?]

**Recommendation:**
- **BUY** / **HOLD** / **AVOID**
- [Brief reasoning in 2-3 lines]

Keep it concise and actionable for a 2-4 month position trade.
""")
        
        return '\n'.join(prompt_parts)
    
    def get_fundamentals_from_knowledge(self, symbol: str) -> Optional[Dict]:
        """Extract fundamental metrics from knowledge file (fallback)."""
        content = self.kb.get_stock(symbol)
        if not content:
            return None
        
        metrics = {}
        in_metrics = False
        
        for line in content.split('\n'):
            if '## Key Metrics' in line:
                in_metrics = True
                continue
            if in_metrics and line.startswith('##'):
                break
            if in_metrics and ':' in line and '**' in line:
                try:
                    key = line.split('**')[1].replace(':', '').strip()
                    value = line.split(':')[-1].strip()
                    metrics[key] = value
                except:
                    pass
        
        return metrics if metrics else None
    
    def analyze(self, symbol: str) -> str:
        """
        Perform comprehensive fundamental analysis on a stock.
        """
        logger.info(f"Running Fundamental Analysis for {symbol}")
        
        # 1. Fetch comprehensive data
        data = self.fetcher.fetch_all(symbol)
        
        # 2. Build prompt
        prompt = self._build_analysis_prompt(symbol, data)
        
        # 3. Call LLM
        analysis = self._call_llm(prompt)
        
        return analysis
    
    def update_knowledge_file(self, symbol: str) -> bool:
        """Run analysis and update knowledge file."""
        analysis = self.analyze(symbol)
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        section_content = f"*Updated: {timestamp}*\n\n{analysis}"
        
        if self.kb.update_section(symbol, "Agent: Fundamental Analysis", section_content):
            logger.success(f"Updated {symbol}.md with Fundamental Analysis")
            return True
        else:
            logger.error(f"Failed to update {symbol}.md")
            return False


# CLI for testing
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fundamental Analysis Agent")
    parser.add_argument("symbol", type=str, help="Stock symbol to analyze")
    
    args = parser.parse_args()
    
    agent = FundamentalAgent()
    agent.update_knowledge_file(args.symbol.upper())
