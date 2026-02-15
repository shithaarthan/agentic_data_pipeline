"""
Lakehouse Pipeline - End-to-End Data Pipeline
Orchestrates Bronze → Silver → Gold transformations.
"""

import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Optional
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from lakehouse.iceberg_catalog import init_lakehouse_tables, get_catalog
from lakehouse.bronze import BronzeIngestion
from lakehouse.silver import SilverTransformation
from lakehouse.gold import GoldAnalytics


class LakehousePipeline:
    """
    End-to-end pipeline for the Foxa Data Lakehouse.
    
    Pipeline Flow:
    1. Bronze: Ingest raw data from sources
    2. Silver: Clean, validate, enrich
    3. Gold: Generate analytics and signals
    """
    
    def __init__(self):
        self.catalog = get_catalog()
        self.bronze = BronzeIngestion()
        self.silver = SilverTransformation()
        self.gold = GoldAnalytics()
    
    def initialize(self):
        """Initialize all lakehouse tables."""
        logger.info("Initializing Lakehouse tables...")
        init_lakehouse_tables()
        logger.success("Lakehouse initialized!")
    
    def run_bronze_layer(
        self,
        symbols: Optional[List[str]] = None,
        ingest_ohlcv: bool = True,
        ingest_fundamentals: bool = True
    ) -> Dict[str, any]:
        """
        Run Bronze layer ingestion.
        
        Returns:
            Dict with ingestion stats
        """
        logger.info("=" * 60)
        logger.info("BRONZE LAYER - Raw Data Ingestion")
        logger.info("=" * 60)
        
        results = {}
        
        if ingest_ohlcv:
            logger.info("Ingesting OHLCV data...")
            ohlcv_results = self.bronze.ingest_ohlcv_batch(symbols)
            results['ohlcv'] = {
                'symbols_processed': len(ohlcv_results),
                'total_rows': sum(ohlcv_results.values()),
                'details': ohlcv_results
            }
        
        if ingest_fundamentals:
            logger.info("Ingesting fundamental data...")
            # Get symbols from OHLCV if not specified
            if symbols is None:
                symbols = self.bronze.ohlcv_storage.list_symbols()[:100]  # Limit for testing
            
            fund_results = self.bronze.ingest_fundamentals_batch(symbols[:50])  # Limit API calls
            results['fundamentals'] = {
                'symbols_processed': len(fund_results),
                'successful': sum(fund_results.values()),
                'details': fund_results
            }
        
        logger.success(f"Bronze layer complete: {results}")
        return results
    
    def run_silver_layer(
        self,
        symbols: Optional[List[str]] = None,
        transform_ohlcv: bool = True,
        calculate_indicators: bool = True
    ) -> Dict[str, int]:
        """
        Run Silver layer transformations.
        
        Returns:
            Dict with row counts
        """
        logger.info("=" * 60)
        logger.info("SILVER LAYER - Data Transformation")
        logger.info("=" * 60)
        
        results = self.silver.run_full_silver_pipeline(symbols)
        
        logger.success(f"Silver layer complete: {results}")
        return results
    
    def run_gold_layer(
        self,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, any]:
        """
        Run Gold layer analytics.
        
        Returns:
            Dict with analytics results
        """
        logger.info("=" * 60)
        logger.info("GOLD LAYER - Analytics Generation")
        logger.info("=" * 60)
        
        results = self.gold.run_full_gold_pipeline(symbols)
        
        logger.success(f"Gold layer complete: {results}")
        return results
    
    def run_full_pipeline(
        self,
        symbols: Optional[List[str]] = None,
        skip_bronze: bool = False,
        skip_silver: bool = False,
        skip_gold: bool = False
    ) -> Dict[str, any]:
        """
        Run complete Bronze → Silver → Gold pipeline.
        
        Args:
            symbols: List of symbols to process. None = all available.
            skip_bronze: Skip bronze ingestion (use existing data)
            skip_silver: Skip silver transformation
            skip_gold: Skip gold analytics
            
        Returns:
            Complete pipeline results
        """
        start_time = datetime.now()
        
        logger.info("🚀 Starting Full Lakehouse Pipeline")
        logger.info(f"   Start time: {start_time}")
        logger.info("-" * 60)
        
        results = {
            'pipeline_start': start_time.isoformat(),
            'symbols': symbols,
            'bronze': None,
            'silver': None,
            'gold': None
        }
        
        # Phase 1: Bronze
        if not skip_bronze:
            results['bronze'] = self.run_bronze_layer(symbols)
        else:
            logger.info("Skipping Bronze layer (using existing data)")
        
        # Phase 2: Silver
        if not skip_silver:
            results['silver'] = self.run_silver_layer(symbols)
        else:
            logger.info("Skipping Silver layer (using existing data)")
        
        # Phase 3: Gold
        if not skip_gold:
            results['gold'] = self.run_gold_layer(symbols)
        else:
            logger.info("Skipping Gold layer")
        
        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        results['pipeline_end'] = end_time.isoformat()
        results['duration_seconds'] = duration
        
        logger.info("=" * 60)
        logger.success(f"✅ Pipeline Complete in {duration:.1f}s")
        logger.info("=" * 60)
        
        return results
    
    def get_pipeline_status(self) -> Dict[str, any]:
        """Get current status of all lakehouse tables."""
        status = {
            'tables': {},
            'catalog': self.catalog.catalog_name,
            'timestamp': datetime.now().isoformat()
        }
        
        namespaces = ['bronze', 'silver', 'gold']
        
        for ns in namespaces:
            try:
                tables = self.catalog.list_tables(ns)
                status['tables'][ns] = {}
                
                for table_name in tables:
                    try:
                        table = self.catalog.load_table(f"{ns}.{table_name}")
                        if table:
                            status['tables'][ns][table_name] = {
                                'location': table.location,
                                'snapshots': len(table.snapshots())
                            }
                    except Exception as e:
                        status['tables'][ns][table_name] = {'error': str(e)}
                        
            except Exception as e:
                status['tables'][ns] = {'error': str(e)}
        
        return status


# ============================================================
# CLI Interface
# ============================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Foxa Lakehouse Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize tables only
  python -m lakehouse.pipeline --init
  
  # Full pipeline for all symbols
  python -m lakehouse.pipeline --full
  
  # Bronze only
  python -m lakehouse.pipeline --bronze
  
  # Skip bronze, run silver+gold
  python -m lakehouse.pipeline --silver --gold
  
  # Specific symbols
  python -m lakehouse.pipeline --full --symbols RELIANCE TCS INFY
        """
    )
    
    parser.add_argument("--init", action="store_true", help="Initialize tables only")
    parser.add_argument("--full", action="store_true", help="Run full pipeline")
    parser.add_argument("--bronze", action="store_true", help="Run bronze layer")
    parser.add_argument("--silver", action="store_true", help="Run silver layer")
    parser.add_argument("--gold", action="store_true", help="Run gold layer")
    parser.add_argument("--symbols", nargs='+', help="Specific symbols to process")
    parser.add_argument("--status", action="store_true", help="Show pipeline status")
    parser.add_argument("--skip-bronze", action="store_true", help="Skip bronze ingestion")
    
    args = parser.parse_args()
    
    pipeline = LakehousePipeline()
    
    symbols = [s.upper() for s in args.symbols] if args.symbols else None
    
    if args.init:
        pipeline.initialize()
    
    elif args.status:
        status = pipeline.get_pipeline_status()
        import json
        print(json.dumps(status, indent=2, default=str))
    
    elif args.full:
        results = pipeline.run_full_pipeline(
            symbols=symbols,
            skip_bronze=args.skip_bronze
        )
        print("\n" + "=" * 60)
        print("Pipeline Results Summary:")
        print("=" * 60)
        import json
        print(json.dumps(results, indent=2, default=str))
    
    elif args.bronze:
        results = pipeline.run_bronze_layer(symbols)
        print(f"\nBronze Results: {results}")
    
    elif args.silver:
        results = pipeline.run_silver_layer(symbols)
        print(f"\nSilver Results: {results}")
    
    elif args.gold:
        results = pipeline.run_gold_layer(symbols)
        print(f"\nGold Results: {results}")
    
    else:
        parser.print_help()