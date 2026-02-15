"""
Memory layer for the Trading Assistant.
Provides both short-term (conversation) and long-term (persistent) memory.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import json
import uuid
from loguru import logger

from database import get_db_session, ConversationMemory, LongTermMemory


@dataclass
class Message:
    """A single message in the conversation."""
    role: str  # "user", "assistant", "system"
    content: str
    timestamp: datetime = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "role": self.role,
            "content": self.content,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "metadata": self.metadata
        }
    
    def to_llm_format(self) -> Dict[str, str]:
        """Format for LLM consumption (OpenAI/Ollama style)."""
        return {"role": self.role, "content": self.content}


@dataclass
class MemoryEntry:
    """A long-term memory entry."""
    memory_type: str
    key: str
    content: str
    importance: float = 0.5
    metadata: Dict[str, Any] = None


class BaseMemory(ABC):
    """Abstract base class for memory implementations."""
    
    @abstractmethod
    def add(self, message: Message):
        """Add a message to memory."""
        pass
    
    @abstractmethod
    def get_recent(self, limit: int = 10) -> List[Message]:
        """Get recent messages."""
        pass
    
    @abstractmethod
    def clear(self):
        """Clear all memory."""
        pass


class ConversationMemoryManager:
    """
    Manages short-term conversation memory.
    Stores recent chat messages for context.
    """
    
    def __init__(self, session_id: str = None, max_messages: int = 50):
        """
        Initialize conversation memory.
        
        Args:
            session_id: Unique session identifier. Auto-generated if not provided.
            max_messages: Maximum messages to retain in memory.
        """
        self.session_id = session_id or str(uuid.uuid4())
        self.max_messages = max_messages
        self._buffer: List[Message] = []
        
        # Load existing messages for this session
        self._load_from_db()
    
    def _load_from_db(self):
        """Load existing messages from database."""
        try:
            with get_db_session() as db:
                messages = db.query(ConversationMemory).filter(
                    ConversationMemory.session_id == self.session_id
                ).order_by(ConversationMemory.timestamp.desc()).limit(self.max_messages).all()
                
                for msg in reversed(messages):
                    self._buffer.append(Message(
                        role=msg.role,
                        content=msg.content,
                        timestamp=msg.timestamp,
                        metadata=msg.extra_data or {}
                    ))
        except Exception as e:
            logger.warning(f"Could not load conversation history: {e}")
    
    def add(self, role: str, content: str, metadata: Dict[str, Any] = None):
        """
        Add a message to the conversation.
        
        Args:
            role: "user", "assistant", or "system"
            content: Message content
            metadata: Optional metadata
        """
        message = Message(role=role, content=content, metadata=metadata)
        self._buffer.append(message)
        
        # Persist to database
        try:
            with get_db_session() as db:
                db_message = ConversationMemory(
                    session_id=self.session_id,
                    role=role,
                    content=content,
                    extra_data=metadata
                )
                db.add(db_message)
        except Exception as e:
            logger.warning(f"Could not persist message: {e}")
        
        # Trim if exceeding max
        if len(self._buffer) > self.max_messages:
            self._buffer = self._buffer[-self.max_messages:]
    
    def add_user_message(self, content: str, metadata: Dict[str, Any] = None):
        """Add a user message."""
        self.add("user", content, metadata)
    
    def add_assistant_message(self, content: str, metadata: Dict[str, Any] = None):
        """Add an assistant message."""
        self.add("assistant", content, metadata)
    
    def add_system_message(self, content: str, metadata: Dict[str, Any] = None):
        """Add a system message."""
        self.add("system", content, metadata)
    
    def get_recent(self, limit: int = 10) -> List[Message]:
        """Get recent messages."""
        return self._buffer[-limit:]
    
    def get_context_for_llm(self, limit: int = 10, include_system: bool = True) -> List[Dict[str, str]]:
        """
        Get messages formatted for LLM consumption.
        
        Args:
            limit: Number of recent messages to include
            include_system: Whether to include system messages
        
        Returns:
            List of {"role": ..., "content": ...} dicts
        """
        messages = self.get_recent(limit)
        if not include_system:
            messages = [m for m in messages if m.role != "system"]
        return [m.to_llm_format() for m in messages]
    
    def get_summary(self) -> str:
        """Get a summary of the conversation for context."""
        if not self._buffer:
            return "No previous conversation."
        
        # Simple summary: last few exchanges
        recent = self._buffer[-6:]  # Last 3 exchanges
        summary_parts = []
        for msg in recent:
            prefix = "User:" if msg.role == "user" else "Assistant:"
            summary_parts.append(f"{prefix} {msg.content[:100]}...")
        
        return "\n".join(summary_parts)
    
    def clear(self):
        """Clear conversation memory."""
        self._buffer = []
        try:
            with get_db_session() as db:
                db.query(ConversationMemory).filter(
                    ConversationMemory.session_id == self.session_id
                ).delete()
        except Exception as e:
            logger.warning(f"Could not clear conversation: {e}")
    
    def search(self, query: str, limit: int = 5) -> List[Message]:
        """Simple search through messages (basic keyword matching)."""
        query_lower = query.lower()
        matches = [
            m for m in self._buffer 
            if query_lower in m.content.lower()
        ]
        return matches[-limit:]


class LongTermMemoryManager:
    """
    Manages long-term persistent memory.
    Stores learnings, patterns, and insights.
    """
    
    # Memory types
    TRADE_OUTCOME = "trade_outcome"
    MARKET_PATTERN = "market_pattern"
    USER_PREFERENCE = "user_preference"
    STRATEGY_INSIGHT = "strategy_insight"
    
    def __init__(self):
        pass
    
    def store(
        self, 
        memory_type: str, 
        key: str, 
        content: str, 
        importance: float = 0.5,
        embedding: List[float] = None
    ):
        """
        Store a long-term memory.
        
        Args:
            memory_type: Type of memory (use class constants)
            key: Unique key for this memory
            content: The memory content
            importance: Importance score (0-1)
            embedding: Optional vector embedding for similarity search
        """
        try:
            with get_db_session() as db:
                # Check if exists
                existing = db.query(LongTermMemory).filter(
                    LongTermMemory.memory_type == memory_type,
                    LongTermMemory.key == key
                ).first()
                
                if existing:
                    # Update existing
                    existing.content = content
                    existing.importance = importance
                    existing.updated_at = datetime.utcnow()
                    if embedding:
                        existing.embedding = embedding
                else:
                    # Create new
                    memory = LongTermMemory(
                        memory_type=memory_type,
                        key=key,
                        content=content,
                        importance=importance,
                        embedding=embedding
                    )
                    db.add(memory)
                
                logger.debug(f"Stored memory: {memory_type}/{key}")
        except Exception as e:
            logger.error(f"Failed to store memory: {e}")
    
    def retrieve(self, memory_type: str = None, key: str = None) -> List[MemoryEntry]:
        """
        Retrieve memories by type and/or key.
        
        Args:
            memory_type: Filter by type
            key: Filter by key (partial match)
        
        Returns:
            List of MemoryEntry objects
        """
        try:
            with get_db_session() as db:
                query = db.query(LongTermMemory)
                
                if memory_type:
                    query = query.filter(LongTermMemory.memory_type == memory_type)
                
                if key:
                    query = query.filter(LongTermMemory.key.contains(key))
                
                # Update access stats
                memories = query.all()
                for m in memories:
                    m.access_count += 1
                    m.last_accessed = datetime.utcnow()
                
                return [
                    MemoryEntry(
                        memory_type=m.memory_type,
                        key=m.key,
                        content=m.content,
                        importance=m.importance
                    )
                    for m in memories
                ]
        except Exception as e:
            logger.error(f"Failed to retrieve memories: {e}")
            return []
    
    def get_relevant_context(self, query: str, limit: int = 5) -> str:
        """
        Get relevant memories as context for LLM.
        Basic keyword matching (will be enhanced with embeddings in Phase 3).
        
        Args:
            query: Search query
            limit: Max memories to return
        
        Returns:
            Formatted string of relevant memories
        """
        try:
            with get_db_session() as db:
                # Simple keyword search
                query_lower = query.lower()
                all_memories = db.query(LongTermMemory).all()
                
                # Score by relevance (basic)
                scored = []
                for m in all_memories:
                    score = 0
                    if query_lower in m.content.lower():
                        score += 2
                    if query_lower in m.key.lower():
                        score += 3
                    score += m.importance
                    if score > 0:
                        scored.append((score, m))
                
                # Sort by score and take top
                scored.sort(key=lambda x: x[0], reverse=True)
                top_memories = scored[:limit]
                
                if not top_memories:
                    return ""
                
                # Format for context
                context_parts = ["Relevant memories:"]
                for score, m in top_memories:
                    context_parts.append(f"- [{m.memory_type}] {m.key}: {m.content}")
                
                return "\n".join(context_parts)
        except Exception as e:
            logger.error(f"Failed to get context: {e}")
            return ""
    
    def store_trade_outcome(self, symbol: str, outcome: str, lessons: str):
        """Store outcome of a trade for learning."""
        key = f"{symbol}_{datetime.now().strftime('%Y%m%d')}"
        content = f"Outcome: {outcome}. Lessons: {lessons}"
        self.store(self.TRADE_OUTCOME, key, content, importance=0.7)
    
    def store_user_preference(self, preference_key: str, value: str):
        """Store user preference."""
        self.store(self.USER_PREFERENCE, preference_key, value, importance=0.8)
    
    def get_user_preferences(self) -> Dict[str, str]:
        """Get all user preferences."""
        memories = self.retrieve(memory_type=self.USER_PREFERENCE)
        return {m.key: m.content for m in memories}
    
    def prune_old_memories(self, days: int = 90, min_importance: float = 0.3):
        """Remove old, low-importance memories."""
        try:
            with get_db_session() as db:
                cutoff = datetime.utcnow() - timedelta(days=days)
                deleted = db.query(LongTermMemory).filter(
                    LongTermMemory.updated_at < cutoff,
                    LongTermMemory.importance < min_importance,
                    LongTermMemory.access_count < 5
                ).delete()
                logger.info(f"Pruned {deleted} old memories")
        except Exception as e:
            logger.error(f"Failed to prune memories: {e}")


class TradingMemory:
    """
    Unified memory interface for the Trading Assistant.
    Combines conversation and long-term memory.
    """
    
    def __init__(self, session_id: str = None):
        """
        Initialize trading memory.
        
        Args:
            session_id: Session ID for conversation tracking
        """
        self.conversation = ConversationMemoryManager(session_id)
        self.long_term = LongTermMemoryManager()
    
    def get_full_context(self, query: str, conversation_limit: int = 5) -> str:
        """
        Get combined context from conversation and long-term memory.
        
        Args:
            query: Current query/context
            conversation_limit: Number of recent messages to include
        
        Returns:
            Formatted context string
        """
        parts = []
        
        # Recent conversation
        conv_summary = self.conversation.get_summary()
        if conv_summary and conv_summary != "No previous conversation.":
            parts.append(f"Recent conversation:\n{conv_summary}")
        
        # Relevant long-term memories
        ltm_context = self.long_term.get_relevant_context(query)
        if ltm_context:
            parts.append(ltm_context)
        
        # User preferences
        prefs = self.long_term.get_user_preferences()
        if prefs:
            pref_str = ", ".join(f"{k}={v}" for k, v in list(prefs.items())[:5])
            parts.append(f"User preferences: {pref_str}")
        
        return "\n\n".join(parts) if parts else ""


# Usage example
if __name__ == "__main__":
    from database import init_db
    init_db()
    
    # Test conversation memory
    print("=== Testing Conversation Memory ===")
    conv = ConversationMemoryManager()
    conv.add_user_message("Analyze RELIANCE for me")
    conv.add_assistant_message("RELIANCE is showing bullish signals...")
    conv.add_user_message("What about TCS?")
    
    print("Recent messages:")
    for msg in conv.get_recent(5):
        print(f"  [{msg.role}]: {msg.content[:50]}...")
    
    print("\nLLM format:")
    print(conv.get_context_for_llm())
    
    # Test long-term memory
    print("\n=== Testing Long-term Memory ===")
    ltm = LongTermMemoryManager()
    ltm.store_user_preference("risk_tolerance", "moderate")
    ltm.store_user_preference("preferred_sectors", "IT, Banking")
    ltm.store_trade_outcome("RELIANCE", "Profit +5%", "Wait for confirmation")
    
    print("User preferences:")
    print(ltm.get_user_preferences())
    
    print("\nRelevant context for 'RELIANCE':")
    print(ltm.get_relevant_context("RELIANCE"))
