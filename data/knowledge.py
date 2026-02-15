"""
Knowledge Base Reader
Molt Bot-style markdown knowledge system for trading.
"""

import os
from pathlib import Path
from typing import Optional, List, Dict
from loguru import logger


# Base path for knowledge files
KNOWLEDGE_BASE = Path(__file__).parent.parent / "knowledge"


class KnowledgeReader:
    """
    Reads markdown knowledge files for agents.
    Molt Bot-style: simple file-based, human-readable.
    """
    
    def __init__(self, base_path: Path = None):
        self.base_path = base_path or KNOWLEDGE_BASE
        self.stocks_path = self.base_path / "stocks"
        self.sectors_path = self.base_path / "sectors"
        self.strategies_path = self.base_path / "strategies"
        self.memory_file = self.base_path / "MEMORY.md"
    
    def _read_file(self, path: Path) -> Optional[str]:
        """Read a markdown file."""
        try:
            if path.exists():
                return path.read_text(encoding="utf-8")
            return None
        except Exception as e:
            logger.warning(f"Could not read {path}: {e}")
            return None
    
    def _write_file(self, path: Path, content: str) -> bool:
        """Write/update a markdown file."""
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            return True
        except Exception as e:
            logger.error(f"Could not write {path}: {e}")
            return False
    
    # --- Stock Knowledge ---
    
    def get_stock(self, symbol: str) -> Optional[str]:
        """Get knowledge for a stock."""
        path = self.stocks_path / f"{symbol.upper()}.md"
        return self._read_file(path)
    
    def stock_exists(self, symbol: str) -> bool:
        """Check if stock knowledge file exists."""
        path = self.stocks_path / f"{symbol.upper()}.md"
        return path.exists()
    
    def list_stocks(self) -> List[str]:
        """List all stocks with knowledge files."""
        if not self.stocks_path.exists():
            return []
        return [f.stem for f in self.stocks_path.glob("*.md")]
    
    def update_stock(self, symbol: str, content: str) -> bool:
        """Update or create stock knowledge file."""
        path = self.stocks_path / f"{symbol.upper()}.md"
        return self._write_file(path, content)
    
    def append_to_stock(self, symbol: str, section: str, text: str) -> bool:
        """Append text to a section in stock file."""
        content = self.get_stock(symbol)
        if not content:
            return False
        
        # Find section and append
        section_marker = f"## {section}"
        if section_marker in content:
            # Append after section header
            parts = content.split(section_marker)
            if len(parts) >= 2:
                # Find next section or end
                rest = parts[1]
                next_section = rest.find("\n## ")
                if next_section != -1:
                    insert_point = next_section
                else:
                    insert_point = len(rest)
                
                new_rest = rest[:insert_point] + f"\n- {text}" + rest[insert_point:]
                new_content = parts[0] + section_marker + new_rest
                return self.update_stock(symbol, new_content)
        
        return False

    def update_section(self, symbol: str, section_title: str, content_body: str) -> bool:
        """
        Update or create a specific section in the stock markdown file.
        Used by Agents to overwrite their specific section.
        """
        file_content = self.get_stock(symbol)
        if not file_content:
            logger.warning(f"File for {symbol} not found")
            return False
        
        section_header = f"## {section_title}"
        new_section = f"{section_header}\n{content_body}\n"
        
        if section_header in file_content:
            # Replace existing section
            start_idx = file_content.find(section_header)
            # Find start of next section
            next_section_idx = file_content.find("\n## ", start_idx + len(section_header))
            
            if next_section_idx == -1:
                # It's the last section
                new_content = file_content[:start_idx] + new_section
            else:
                # It's in the middle
                new_content = file_content[:start_idx] + new_section + file_content[next_section_idx+1:]
        else:
            # Append to end
            new_content = file_content.strip() + "\n\n" + new_section
            
        return self.update_stock(symbol, new_content)
    
    # --- Sector Knowledge ---
    
    def get_sector(self, sector: str) -> Optional[str]:
        """Get knowledge for a sector."""
        # Normalize sector name
        filename = sector.replace(" ", "_").replace("&", "and")
        path = self.sectors_path / f"{filename}.md"
        
        if not path.exists():
            # Try common aliases
            aliases = {
                "information technology": "IT",
                "financial services": "Banking",
                "oil & gas": "OilGas",
                "oil and gas": "OilGas",
            }
            alt = aliases.get(sector.lower())
            if alt:
                path = self.sectors_path / f"{alt}.md"
        
        return self._read_file(path)
    
    def list_sectors(self) -> List[str]:
        """List all sectors with knowledge files."""
        if not self.sectors_path.exists():
            return []
        return [f.stem for f in self.sectors_path.glob("*.md")]
    
    # --- Strategy Knowledge ---
    
    def get_strategy(self, strategy: str) -> Optional[str]:
        """Get knowledge for a trading strategy."""
        filename = strategy.lower().replace(" ", "_")
        path = self.strategies_path / f"{filename}.md"
        return self._read_file(path)
    
    def list_strategies(self) -> List[str]:
        """List all strategies."""
        if not self.strategies_path.exists():
            return []
        return [f.stem for f in self.strategies_path.glob("*.md")]
    
    # --- Memory ---
    
    def get_memory(self) -> Optional[str]:
        """Get long-term trading memory."""
        return self._read_file(self.memory_file)
    
    def append_to_memory(self, section: str, text: str) -> bool:
        """Append to a section in memory file."""
        content = self.get_memory()
        if not content:
            return False
        
        section_marker = f"## {section}"
        if section_marker in content:
            # Find the section and append
            lines = content.split("\n")
            new_lines = []
            in_section = False
            appended = False
            
            for line in lines:
                new_lines.append(line)
                if line.startswith(section_marker):
                    in_section = True
                elif in_section and line.startswith("## "):
                    # Next section, insert before
                    if not appended:
                        new_lines.insert(-1, f"- {text}")
                        appended = True
                    in_section = False
                elif in_section and line.startswith("<!--"):
                    # After comment, good place to append
                    if not appended:
                        new_lines.append(f"- {text}")
                        appended = True
            
            if in_section and not appended:
                new_lines.append(f"- {text}")
            
            return self._write_file(self.memory_file, "\n".join(new_lines))
        
        return False
    
    def record_trade_outcome(self, symbol: str, outcome: str, notes: str):
        """Record a trade outcome in memory."""
        from datetime import datetime
        date = datetime.now().strftime("%Y-%m-%d")
        
        section = "Successful Trades" if "profit" in outcome.lower() else "Failed Trades"
        entry = f"[{date}] {symbol}: {outcome} - {notes}"
        self.append_to_memory(section, entry)
    
    # --- Context for Agent ---
    
    def get_context_for_symbol(self, symbol: str, include_sector: bool = True) -> str:
        """
        Get all relevant context for a symbol.
        This is what gets passed to the LLM agent.
        """
        parts = []
        
        # Stock knowledge
        stock_knowledge = self.get_stock(symbol)
        if stock_knowledge:
            parts.append(f"=== Stock Knowledge: {symbol} ===\n{stock_knowledge}")
        
        # Get sector from DB if available
        if include_sector:
            try:
                from database import get_db_session, Stock
                with get_db_session() as db:
                    stock = db.query(Stock).filter_by(symbol=symbol).first()
                    if stock and stock.sector:
                        sector_knowledge = self.get_sector(stock.sector)
                        if sector_knowledge:
                            parts.append(f"=== Sector: {stock.sector} ===\n{sector_knowledge}")
            except Exception as e:
                logger.debug(f"Could not get sector for {symbol}: {e}")
        
        # Memory snippets (abbreviated)
        memory = self.get_memory()
        if memory:
            # Extract just the relevant parts
            if symbol in memory:
                parts.append(f"=== Memory mentions {symbol} ===")
                for line in memory.split("\n"):
                    if symbol in line:
                        parts.append(line)
        
        return "\n\n".join(parts) if parts else ""
    
    def search(self, query: str, limit: int = 5) -> List[Dict[str, str]]:
        """
        Simple keyword search across all knowledge files.
        Returns list of {file, snippet} matches.
        """
        results = []
        query_lower = query.lower()
        
        # Search all markdown files
        for md_file in self.base_path.rglob("*.md"):
            content = self._read_file(md_file)
            if content and query_lower in content.lower():
                # Extract relevant snippet
                lines = content.split("\n")
                for i, line in enumerate(lines):
                    if query_lower in line.lower():
                        start = max(0, i - 1)
                        end = min(len(lines), i + 3)
                        snippet = "\n".join(lines[start:end])
                        results.append({
                            "file": str(md_file.relative_to(self.base_path)),
                            "snippet": snippet[:300]
                        })
                        break
        
        return results[:limit]


# Convenience instance
knowledge = KnowledgeReader()


# Usage example
if __name__ == "__main__":
    kb = KnowledgeReader()
    
    print("=== Available Stocks ===")
    print(kb.list_stocks())
    
    print("\n=== Available Sectors ===")
    print(kb.list_sectors())
    
    print("\n=== RELIANCE Knowledge ===")
    print(kb.get_stock("RELIANCE")[:500] if kb.get_stock("RELIANCE") else "Not found")
    
    print("\n=== Context for TCS ===")
    print(kb.get_context_for_symbol("TCS")[:500])
    
    print("\n=== Search 'breakout' ===")
    for r in kb.search("breakout"):
        print(f"  {r['file']}: {r['snippet'][:100]}...")
