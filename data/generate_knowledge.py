"""
Generate stock knowledge templates from database.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db_session, Stock
from data.knowledge import KnowledgeReader


STOCK_TEMPLATE = """# {symbol}

## Basic Info
- **Name:** {name}
- **Sector:** {sector}
- **Industry:** {industry}
- **Index:** {index}

## Key Metrics
<!-- Add P/E, P/B, etc. -->

## Fundamentals
<!-- Add business overview -->

## Technical Notes
<!-- Support/resistance levels -->

## News & Events
<!-- Recent news -->

## Trading Notes
<!-- Your observations -->

## Risks
<!-- Key risks -->
"""


def generate_stock_templates(overwrite: bool = False):
    """Generate markdown templates for ALL Nifty 500 stocks."""
    kb = KnowledgeReader()
    
    with get_db_session() as db:
        # Get ALL stocks, not just Nifty 50
        stocks = db.query(Stock).filter_by(is_active=True).all()
        
        created = 0
        skipped = 0
        
        for stock in stocks:
            if not overwrite and kb.stock_exists(stock.symbol):
                skipped += 1
                continue
            
            # Determine index membership
            index_parts = []
            if stock.is_nifty50:
                index_parts.append("Nifty 50")
            if stock.is_nifty100:
                index_parts.append("Nifty 100")
            if stock.is_nifty500:
                index_parts.append("Nifty 500")
            
            content = STOCK_TEMPLATE.format(
                symbol=stock.symbol,
                name=stock.name or stock.symbol,
                sector=stock.sector or "Unknown",
                industry=stock.industry or "Unknown",
                index=", ".join(index_parts) if index_parts else "Nifty 500"
            )
            
            if kb.update_stock(stock.symbol, content):
                created += 1
                print(f"Created: {stock.symbol}")
        
        print(f"\nCreated: {created}, Skipped: {skipped}")
        return created


if __name__ == "__main__":
    print("Generating stock knowledge templates for Nifty 50...")
    generate_stock_templates()
    
    print("\nFiles created in: knowledge/stocks/")
