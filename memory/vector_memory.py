"""
Vector Memory Module with ChromaDB
Provides semantic search capabilities for memory retrieval.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib
from loguru import logger

try:
    import chromadb
    from chromadb.config import Settings
    HAS_CHROMADB = True
except ImportError:
    HAS_CHROMADB = False
    logger.warning("ChromaDB not installed. Vector memory features disabled.")

try:
    from sentence_transformers import SentenceTransformer
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False
    logger.warning("sentence-transformers not installed. Using fallback embeddings.")


class EmbeddingModel:
    """
    Embedding model wrapper.
    Uses sentence-transformers if available, otherwise falls back to simple hashing.
    """
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model_name = model_name
        self._model = None
        
        if HAS_SENTENCE_TRANSFORMERS:
            try:
                logger.info(f"Loading embedding model: {model_name}")
                self._model = SentenceTransformer(model_name)
                logger.success("Embedding model loaded")
            except Exception as e:
                logger.warning(f"Could not load embedding model: {e}")
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of texts."""
        if self._model is not None:
            embeddings = self._model.encode(texts, convert_to_numpy=True)
            return embeddings.tolist()
        else:
            # Fallback: simple hash-based pseudo-embeddings (dims=384 to match MiniLM)
            logger.debug("Using fallback hash-based embeddings")
            embeddings = []
            for text in texts:
                # Create deterministic pseudo-embedding from text hash
                hash_bytes = hashlib.sha384(text.encode()).digest()
                # Convert to floats between -1 and 1
                embedding = [(b - 128) / 128.0 for b in hash_bytes]
                embeddings.append(embedding)
            return embeddings
    
    def embed_single(self, text: str) -> List[float]:
        """Generate embedding for a single text."""
        return self.embed([text])[0]


class VectorMemory:
    """
    Vector-based memory using ChromaDB for semantic search.
    Stores memories with embeddings for similarity retrieval.
    """
    
    def __init__(
        self, 
        collection_name: str = "trading_memory",
        persist_directory: str = "./chroma_db"
    ):
        """
        Initialize vector memory.
        
        Args:
            collection_name: Name of the ChromaDB collection
            persist_directory: Directory to persist the vector database
        """
        self.collection_name = collection_name
        self.persist_directory = persist_directory
        self._embedding_model = EmbeddingModel()
        self._client = None
        self._collection = None
        
        if HAS_CHROMADB:
            self._init_chromadb()
        else:
            logger.warning("ChromaDB not available. Vector memory will be in-memory only.")
            self._fallback_store: List[Dict] = []
    
    def _init_chromadb(self):
        """Initialize ChromaDB client and collection."""
        try:
            self._client = chromadb.PersistentClient(path=self.persist_directory)
            
            # Get or create collection
            self._collection = self._client.get_or_create_collection(
                name=self.collection_name,
                metadata={"description": "Trading assistant memories"}
            )
            
            logger.success(f"ChromaDB initialized with collection: {self.collection_name}")
        except Exception as e:
            logger.error(f"Failed to initialize ChromaDB: {e}")
            self._fallback_store = []
    
    def add(
        self, 
        text: str, 
        metadata: Dict[str, Any] = None,
        memory_type: str = "general"
    ) -> str:
        """
        Add a memory to the vector store.
        
        Args:
            text: The memory content
            metadata: Additional metadata
            memory_type: Type of memory (general, trade, insight, etc.)
        
        Returns:
            Memory ID
        """
        # Generate unique ID
        memory_id = hashlib.md5(
            f"{text}{datetime.now().isoformat()}".encode()
        ).hexdigest()[:16]
        
        # Prepare metadata
        meta = metadata or {}
        meta["memory_type"] = memory_type
        meta["timestamp"] = datetime.now().isoformat()
        meta["text_preview"] = text[:100]
        
        if self._collection is not None:
            try:
                # Generate embedding
                embedding = self._embedding_model.embed_single(text)
                
                self._collection.add(
                    ids=[memory_id],
                    embeddings=[embedding],
                    documents=[text],
                    metadatas=[meta]
                )
                logger.debug(f"Added memory: {memory_id}")
            except Exception as e:
                logger.error(f"Failed to add memory: {e}")
        else:
            # Fallback store
            self._fallback_store.append({
                "id": memory_id,
                "text": text,
                "metadata": meta,
                "embedding": self._embedding_model.embed_single(text)
            })
        
        return memory_id
    
    def search(
        self, 
        query: str, 
        limit: int = 5,
        memory_type: str = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar memories.
        
        Args:
            query: Search query
            limit: Maximum results to return
            memory_type: Filter by memory type
        
        Returns:
            List of matching memories with scores
        """
        if self._collection is not None:
            try:
                # Build where clause if filtering
                where = {"memory_type": memory_type} if memory_type else None
                
                # Generate query embedding
                query_embedding = self._embedding_model.embed_single(query)
                
                results = self._collection.query(
                    query_embeddings=[query_embedding],
                    n_results=limit,
                    where=where,
                    include=["documents", "metadatas", "distances"]
                )
                
                # Format results
                formatted = []
                if results["ids"] and results["ids"][0]:
                    for i, doc_id in enumerate(results["ids"][0]):
                        formatted.append({
                            "id": doc_id,
                            "text": results["documents"][0][i],
                            "metadata": results["metadatas"][0][i],
                            "distance": results["distances"][0][i] if results.get("distances") else 0
                        })
                
                return formatted
                
            except Exception as e:
                logger.error(f"Search failed: {e}")
                return []
        else:
            # Fallback search using cosine similarity
            return self._fallback_search(query, limit, memory_type)
    
    def _fallback_search(
        self, 
        query: str, 
        limit: int, 
        memory_type: str = None
    ) -> List[Dict[str, Any]]:
        """Fallback search for when ChromaDB is not available."""
        import numpy as np
        
        query_embedding = np.array(self._embedding_model.embed_single(query))
        
        scored = []
        for item in self._fallback_store:
            if memory_type and item["metadata"].get("memory_type") != memory_type:
                continue
            
            # Cosine similarity
            item_embedding = np.array(item["embedding"])
            similarity = np.dot(query_embedding, item_embedding) / (
                np.linalg.norm(query_embedding) * np.linalg.norm(item_embedding)
            )
            
            scored.append((similarity, item))
        
        # Sort by similarity (descending)
        scored.sort(key=lambda x: x[0], reverse=True)
        
        return [
            {
                "id": item["id"],
                "text": item["text"],
                "metadata": item["metadata"],
                "distance": 1 - score  # Convert similarity to distance
            }
            for score, item in scored[:limit]
        ]
    
    def delete(self, memory_id: str) -> bool:
        """Delete a memory by ID."""
        if self._collection is not None:
            try:
                self._collection.delete(ids=[memory_id])
                return True
            except Exception as e:
                logger.error(f"Delete failed: {e}")
                return False
        else:
            self._fallback_store = [
                m for m in self._fallback_store if m["id"] != memory_id
            ]
            return True
    
    def get_all(self, memory_type: str = None) -> List[Dict[str, Any]]:
        """Get all memories, optionally filtered by type."""
        if self._collection is not None:
            try:
                where = {"memory_type": memory_type} if memory_type else None
                results = self._collection.get(
                    where=where,
                    include=["documents", "metadatas"]
                )
                
                formatted = []
                for i, doc_id in enumerate(results["ids"]):
                    formatted.append({
                        "id": doc_id,
                        "text": results["documents"][i],
                        "metadata": results["metadatas"][i]
                    })
                return formatted
            except Exception as e:
                logger.error(f"Get all failed: {e}")
                return []
        else:
            if memory_type:
                return [
                    m for m in self._fallback_store 
                    if m["metadata"].get("memory_type") == memory_type
                ]
            return self._fallback_store
    
    def count(self) -> int:
        """Get total number of memories."""
        if self._collection is not None:
            return self._collection.count()
        return len(self._fallback_store)


class TradingKnowledgeBase:
    """
    RAG-enabled knowledge base for trading information.
    Stores and retrieves trading strategies, market insights, and learnings.
    """
    
    def __init__(self):
        self.vector_memory = VectorMemory(
            collection_name="trading_knowledge",
            persist_directory="./chroma_db"
        )
    
    def add_trade_insight(self, symbol: str, insight: str, outcome: str = None):
        """Add an insight from a trade."""
        text = f"Trade insight for {symbol}: {insight}"
        if outcome:
            text += f" Outcome: {outcome}"
        
        self.vector_memory.add(
            text=text,
            metadata={"symbol": symbol, "outcome": outcome},
            memory_type="trade_insight"
        )
    
    def add_market_pattern(self, pattern: str, description: str):
        """Add a market pattern observation."""
        text = f"Market pattern - {pattern}: {description}"
        self.vector_memory.add(
            text=text,
            metadata={"pattern": pattern},
            memory_type="market_pattern"
        )
    
    def add_strategy_note(self, strategy: str, note: str):
        """Add a strategy note."""
        text = f"Strategy note for {strategy}: {note}"
        self.vector_memory.add(
            text=text,
            metadata={"strategy": strategy},
            memory_type="strategy"
        )
    
    def add_document(self, title: str, content: str, source: str = None):
        """Add a document to the knowledge base."""
        # Split long documents into chunks
        chunk_size = 500
        chunks = [content[i:i+chunk_size] for i in range(0, len(content), chunk_size)]
        
        for i, chunk in enumerate(chunks):
            self.vector_memory.add(
                text=chunk,
                metadata={"title": title, "source": source, "chunk": i},
                memory_type="document"
            )
    
    def query(self, question: str, limit: int = 5) -> List[Dict]:
        """Query the knowledge base."""
        return self.vector_memory.search(question, limit=limit)
    
    def get_context_for_symbol(self, symbol: str, limit: int = 5) -> str:
        """Get relevant context for a specific symbol."""
        results = self.vector_memory.search(symbol, limit=limit)
        
        if not results:
            return ""
        
        context_parts = [f"Relevant knowledge for {symbol}:"]
        for r in results:
            context_parts.append(f"- {r['text'][:200]}...")
        
        return "\n".join(context_parts)


# Usage example
if __name__ == "__main__":
    print("=== Testing Vector Memory ===")
    
    vm = VectorMemory()
    
    # Add some memories
    vm.add("RELIANCE showed strong bullish momentum with RSI crossing 60", 
           memory_type="trade_insight")
    vm.add("TCS earnings beat expectations, stock up 5% post-results",
           memory_type="trade_insight")
    vm.add("Always wait for confirmation before entering breakout trades",
           memory_type="strategy")
    vm.add("Banking sector tends to rally in December due to credit growth",
           memory_type="market_pattern")
    
    print(f"\nTotal memories: {vm.count()}")
    
    # Search
    print("\n=== Searching for 'RELIANCE' ===")
    results = vm.search("RELIANCE bullish signal")
    for r in results:
        print(f"  - {r['text'][:80]}... (distance: {r['distance']:.3f})")
    
    print("\n=== Searching for 'breakout strategy' ===")
    results = vm.search("breakout trading strategy")
    for r in results:
        print(f"  - {r['text'][:80]}... (distance: {r['distance']:.3f})")
    
    # Test knowledge base
    print("\n=== Testing Knowledge Base ===")
    kb = TradingKnowledgeBase()
    kb.add_trade_insight("INFY", "Strong support at 1450 level", "Bounced +3%")
    kb.add_strategy_note("Breakout", "Wait for volume confirmation above 1.5x average")
    
    print(kb.get_context_for_symbol("INFY"))
