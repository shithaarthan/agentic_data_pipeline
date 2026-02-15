"""
LLM Integration Module
Provides unified interface for various LLM providers (Ollama, OpenAI, Together.ai).
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Generator
from dataclasses import dataclass
from loguru import logger
import json

from config import settings


@dataclass
class ChatMessage:
    """A chat message for LLM."""
    role: str  # "system", "user", "assistant"
    content: str
    
    def to_dict(self) -> Dict[str, str]:
        return {"role": self.role, "content": self.content}


@dataclass
class LLMResponse:
    """Response from LLM."""
    content: str
    model: str
    tokens_used: int = 0
    finish_reason: str = "stop"
    raw_response: Dict = None


class BaseLLM(ABC):
    """Abstract base class for LLM providers."""
    
    @abstractmethod
    def chat(self, messages: List[ChatMessage], **kwargs) -> LLMResponse:
        """Send chat messages and get response."""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if the LLM provider is available."""
        pass


class OllamaLLM(BaseLLM):
    """Ollama LLM provider for local models."""
    
    def __init__(
        self, 
        model: str = None, 
        base_url: str = None,
        temperature: float = 0.7
    ):
        self.model = model or settings.ollama_model
        self.base_url = base_url or settings.ollama_base_url
        self.temperature = temperature
        self._available = None
    
    def is_available(self) -> bool:
        """Check if Ollama is running."""
        if self._available is not None:
            return self._available
        
        try:
            import httpx
            response = httpx.get(f"{self.base_url}/api/tags", timeout=5.0)
            self._available = response.status_code == 200
        except Exception as e:
            logger.debug(f"Ollama not available: {e}")
            self._available = False
        
        return self._available
    
    def chat(self, messages: List[ChatMessage], **kwargs) -> LLMResponse:
        """Send chat to Ollama."""
        import httpx
        
        payload = {
            "model": self.model,
            "messages": [m.to_dict() for m in messages],
            "stream": False,
            "options": {
                "temperature": kwargs.get("temperature", self.temperature)
            }
        }
        
        try:
            response = httpx.post(
                f"{self.base_url}/api/chat",
                json=payload,
                timeout=120.0
            )
            response.raise_for_status()
            data = response.json()
            
            return LLMResponse(
                content=data.get("message", {}).get("content", ""),
                model=self.model,
                tokens_used=data.get("eval_count", 0),
                raw_response=data
            )
        except Exception as e:
            logger.error(f"Ollama chat error: {e}")
            raise


class OpenAILLM(BaseLLM):
    """OpenAI LLM provider."""
    
    def __init__(
        self, 
        model: str = "gpt-4o-mini",
        api_key: str = None,
        temperature: float = 0.7
    ):
        self.model = model
        self.api_key = api_key or settings.openai_api_key
        self.temperature = temperature
    
    def is_available(self) -> bool:
        """Check if OpenAI API key is configured."""
        return bool(self.api_key)
    
    def chat(self, messages: List[ChatMessage], **kwargs) -> LLMResponse:
        """Send chat to OpenAI."""
        import httpx
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": [m.to_dict() for m in messages],
            "temperature": kwargs.get("temperature", self.temperature)
        }
        
        try:
            response = httpx.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=60.0
            )
            response.raise_for_status()
            data = response.json()
            
            choice = data.get("choices", [{}])[0]
            usage = data.get("usage", {})
            
            return LLMResponse(
                content=choice.get("message", {}).get("content", ""),
                model=self.model,
                tokens_used=usage.get("total_tokens", 0),
                finish_reason=choice.get("finish_reason", "stop"),
                raw_response=data
            )
        except Exception as e:
            logger.error(f"OpenAI chat error: {e}")
            raise


class MockLLM(BaseLLM):
    """Mock LLM for testing without API access."""
    
    def __init__(self):
        self.model = "mock-llm"
    
    def is_available(self) -> bool:
        return True
    
    def chat(self, messages: List[ChatMessage], **kwargs) -> LLMResponse:
        """Generate mock trading response."""
        last_message = messages[-1].content if messages else ""
        
        # Extract symbol if mentioned
        symbols = ["RELIANCE", "TCS", "HDFCBANK", "INFY", "SBIN", "ICICIBANK"]
        symbol = "STOCK"
        for s in symbols:
            if s.lower() in last_message.lower():
                symbol = s
                break
        
        import random
        signals = ["BUY", "SELL", "HOLD"]
        signal = random.choice(signals)
        confidence = random.randint(60, 95)
        
        response = f"""Based on my analysis of {symbol}:

**Signal: {signal}** (Confidence: {confidence}%)

**Technical Analysis:**
- RSI: {random.randint(30, 70)} - {'Neutral' if 40 < random.randint(30, 70) < 60 else 'Momentum building'}
- MACD: {'Bullish crossover' if signal == 'BUY' else 'Bearish crossover' if signal == 'SELL' else 'Neutral'}
- Trend: {'Uptrend' if signal == 'BUY' else 'Downtrend' if signal == 'SELL' else 'Sideways'}

**Recommendation:**
{f'Consider buying {symbol} with a stop-loss at 2% below entry.' if signal == 'BUY' else 
 f'Consider selling {symbol} or avoid new positions.' if signal == 'SELL' else 
 f'Hold current position in {symbol}. Wait for clearer signals.'}

**Risk Management:**
- Position size: Max 10% of portfolio
- Stop-loss: 2% from entry
- Target: 4% profit (2:1 risk-reward)

*Note: This is a mock analysis for testing. Not financial advice.*"""
        
        return LLMResponse(
            content=response,
            model="mock-llm",
            tokens_used=0
        )


class LLMManager:
    """
    Manages LLM providers with automatic fallback.
    Tries Ollama first, then OpenAI, then mock.
    """
    
    def __init__(self):
        self.providers: List[BaseLLM] = []
        self.active_provider: Optional[BaseLLM] = None
        
        # Initialize providers in order of preference
        self._init_providers()
    
    def _init_providers(self):
        """Initialize available LLM providers."""
        # Ollama (local, free)
        ollama = OllamaLLM()
        if ollama.is_available():
            self.providers.append(ollama)
            logger.info(f"Ollama available with model: {ollama.model}")
        
        # OpenAI (cloud, paid)
        if settings.has_openai_key:
            openai = OpenAILLM()
            self.providers.append(openai)
            logger.info("OpenAI available")
        
        # Mock (always available for testing)
        self.providers.append(MockLLM())
        
        # Set active provider
        self.active_provider = self.providers[0] if self.providers else MockLLM()
        logger.info(f"Active LLM provider: {self.active_provider.model}")
    
    def chat(self, messages: List[ChatMessage], **kwargs) -> LLMResponse:
        """
        Send chat messages to the active LLM provider.
        Falls back to next provider on failure.
        """
        for provider in self.providers:
            try:
                return provider.chat(messages, **kwargs)
            except Exception as e:
                logger.warning(f"Provider {provider.model} failed: {e}")
                continue
        
        # All providers failed
        raise RuntimeError("All LLM providers failed")
    
    def simple_chat(self, user_message: str, system_prompt: str = None) -> str:
        """
        Simple chat interface.
        
        Args:
            user_message: The user's message
            system_prompt: Optional system prompt
        
        Returns:
            The assistant's response text
        """
        messages = []
        
        if system_prompt:
            messages.append(ChatMessage(role="system", content=system_prompt))
        
        messages.append(ChatMessage(role="user", content=user_message))
        
        response = self.chat(messages)
        return response.content
    
    @property
    def model_name(self) -> str:
        """Get the active model name."""
        return self.active_provider.model if self.active_provider else "none"


# Trading-specific prompts
TRADING_SYSTEM_PROMPT = """You are an expert trading analyst assistant for Indian equity markets (NSE/BSE).

Your role is to:
1. Analyze stocks using technical and fundamental data
2. Provide clear BUY/SELL/HOLD recommendations with confidence levels
3. Explain your reasoning in a structured format
4. Always include risk management guidelines
5. Be honest about uncertainty and limitations

Important guidelines:
- Base analysis on data provided, not speculation
- Always mention that this is not financial advice
- Consider both bullish and bearish scenarios
- Prioritize capital preservation
- Use clear, actionable language

Format your responses with:
- Clear signal (BUY/SELL/HOLD) with confidence %
- Key technical indicators and their interpretation
- Support/resistance levels if available
- Risk management (stop-loss, target, position sizing)
- Summary of reasoning"""


# Usage example
if __name__ == "__main__":
    print("=== Testing LLM Integration ===")
    
    llm = LLMManager()
    print(f"Active provider: {llm.model_name}")
    
    # Test simple chat
    response = llm.simple_chat(
        user_message="Analyze RELIANCE for a potential trade opportunity",
        system_prompt=TRADING_SYSTEM_PROMPT
    )
    
    print("\n=== LLM Response ===")
    print(response)
