"""
Bronze Layer - Raw Data Ingestion
Ingests data from sources into Iceberg tables without transformation.
"""

import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Optional, Iterator
import pandas as pd
import pyarrow as pa
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from lakehouse.iceberg_catalog import get_catalog, TableSchemas
from lakehouse.minio_client import get_minio_client
from data.ohlcv_fetcher import OHLCVStorage
from data.fundamental_fetcher import FundamentalDataFetcher


class BronzeIngestion:
    """
    Handles raw data ingestion into Bronze layer Iceberg tables.
    Bronze = Raw data as-received from sources
    """
    
    def __init__(self):
        self.catalog = get_catalog()
        self.ohlcv_storage = OHLCVStorage()
        self.fundamental_fetcher = FundamentalDataFetcher()
        
    # ============================================================
    # OHLCV Ingestion
    # ============================================================
    
    def ingest_ohlcv_symbol(
        self, 
        symbol: str, 
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """
        Ingest OHLCV data for a single symbol into bronze.ohlcv.
        
        Returns:
            Number of rows ingested
        """
        try:
            # Load from parquet storage
            df = self.ohlcv_storage.load(symbol)
            if df is None or df.empty:
                logger.warning(f"No OHLCV data found for {symbol}")
                return 0
            
            # Prepare dataframe for Iceberg
            df = self._prepare_ohlcv_df(df, symbol)
            
            # Filter by date if specified
            if start_date:
                df = df[df['date'] >= start_date]
            if end_date:
                df = df[df['date'] <= end_date]
            
            if df.empty:
                logger.debug(f"No data in date range for {symbol}")
                return 0
            
            # Append to Iceberg table
            table = self.catalog.load_table("bronze.ohlcv")
            if table is None:
                logger.error("Bronze OHLCV table not found")
                return 0
            
            # Convert to PyArrow and write
            arrow_table = self._df_to_ohlcv_arrow(df)
            
            # Use overwrite for partition to avoid duplicates
            table.overwrite(arrow_table)
            
            count = len(df)
            logger.info(f"Ingested {count} rows for {symbol} into bronze.ohlcv")
            return count
            
        except Exception as e:
            logger.error(f"Failed to ingest OHLCV for {symbol}: {e}")
            return 0
    
    def ingest_ohlcv_batch(
        self, 
        symbols: Optional[List[str]] = None,
        progress_interval: int = 50
    ) -> Dict[str, int]:
        """
        Ingest OHLCV data for multiple symbols.
        
        Args:
            symbols: List of symbols. If None, uses all available parquet files.
            progress_interval: Log progress every N symbols
            
        Returns:
            Dict mapping symbol to row count
        """
        if symbols is None:
            symbols = self.ohlcv_storage.list_symbols()
        
        results = {}
        total_rows = 0
        
        logger.info(f"Starting batch OHLCV ingestion for {len(symbols)} symbols")
        
        for i, symbol in enumerate(symbols, 1):
            count = self.ingest_ohlcv_symbol(symbol)
            results[symbol] = count
            total_rows += count
            
            if i % progress_interval == 0:
                logger.info(f"Progress: {i}/{len(symbols)} symbols processed")
        
        logger.success(f"Batch ingestion complete: {total_rows} total rows from {len(symbols)} symbols")
        return results
    
    def _prepare_ohlcv_df(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Prepare OHLCV dataframe for Iceberg ingestion."""
        df = df.copy()
        
        # Ensure required columns exist
        required = ['open', 'high', 'low', 'close', 'volume']
        for col in required:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")
        
        # Normalize column names
        df = df.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        })
        
        # Ensure index is datetime
        if not isinstance(df.index, pd.DatetimeIndex):
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date')
            else:
                raise ValueError("DataFrame must have DatetimeIndex or 'date' column")
        
        # Add symbol and date columns
        df = df.reset_index()
        df['symbol'] = symbol
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Add timestamp
        df['timestamp'] = pd.to_datetime(df.iloc[:, 0] if 'index' in df.columns else df['date'])
        df['exchange'] = 'NSE'
        
        # Select only required columns
        columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'timestamp', 'exchange']
        df = df[[c for c in columns if c in df.columns]]
        
        # Remove any rows with nulls in required fields
        df = df.dropna(subset=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'])
        
        return df
    
    def _df_to_ohlcv_arrow(self, df: pd.DataFrame) -> pa.Table:
        """Convert DataFrame to PyArrow table with correct schema."""
        # Ensure timestamp is datetime64[us] for Iceberg compatibility
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        arrow_table = pa.Table.from_pandas(df)
        return arrow_table
    
    # ============================================================
    # Fundamentals Ingestion
    # ============================================================
    
    def ingest_fundamentals(self, symbol: str) -> bool:
        """
        Ingest fundamental data for a symbol into bronze.fundamentals.
        
        Returns:
            True if successful
        """
        try:
            # Fetch from cache or API
            data = self.fundamental_fetcher.fetch_all(symbol)
            
            if not data.get('screener') and not data.get('fmp'):
                logger.warning(f"No fundamental data for {symbol}")
                return False
            
            # Extract metrics
            record = self._extract_fundamentals(symbol, data)
            if not record:
                return False
            
            # Create DataFrame
            df = pd.DataFrame([record])
            
            # Append to Iceberg
            table = self.catalog.load_table("bronze.fundamentals")
            if table is None:
                logger.error("Bronze fundamentals table not found")
                return False
            
            arrow_table = pa.Table.from_pandas(df)
            table.overwrite(arrow_table)
            
            logger.info(f"Ingested fundamentals for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to ingest fundamentals for {symbol}: {e}")
            return False
    
    def ingest_fundamentals_batch(
        self, 
        symbols: List[str],
        progress_interval: int = 10
    ) -> Dict[str, bool]:
        """
        Ingest fundamentals for multiple symbols.
        
        Returns:
            Dict mapping symbol to success status
        """
        results = {}
        
        logger.info(f"Starting fundamentals ingestion for {len(symbols)} symbols")
        
        for i, symbol in enumerate(symbols, 1):
            success = self.ingest_fundamentals(symbol)
            results[symbol] = success
            
            if i % progress_interval == 0:
                success_count = sum(results.values())
                logger.info(f"Progress: {i}/{len(symbols)} - {success_count} successful")
        
        success_count = sum(results.values())
        logger.success(f"Fundamentals ingestion: {success_count}/{len(symbols)} successful")
        return results
    
    def _extract_fundamentals(self, symbol: str, data: Dict) -> Optional[Dict]:
        """Extract fundamental metrics from fetched data."""
        record = {
            'symbol': symbol,
            'date': date.today(),
            'updated_at': datetime.now()
        }
        
        # Extract from screener.in data
        screener = data.get('screener', {})
        if screener and 'top_ratios' in screener:
            ratios = screener['top_ratios']
            
            # Parse numeric values from strings like "₹ 13,45,678 Cr" or "28.5"
            record['market_cap'] = self._parse_numeric(ratios.get('Market Cap'))
            record['pe_ratio'] = self._parse_numeric(ratios.get('P/E'))
            record['pb_ratio'] = self._parse_numeric(ratios.get('P/B'))
            record['roe'] = self._parse_numeric(ratios.get('ROE'))
            record['debt_equity'] = self._parse_numeric(ratios.get('Debt to equity'))
        
        # Extract from FMP data
        fmp = data.get('fmp', {})
        if fmp and 'ratios' in fmp:
            ratios = fmp['ratios']
            record['roe'] = record.get('roe') or ratios.get('returnOnEquity')
            record['debt_equity'] = record.get('debt_equity') or ratios.get('debtEquityRatio')
        
        if fmp and 'growth' in fmp:
            growth = fmp['growth']
            record['revenue_growth'] = growth.get('revenueGrowth')
            record['profit_growth'] = growth.get('netIncomeGrowth')
        
        # Determine source
        sources = []
        if screener:
            sources.append('screener')
        if fmp:
            sources.append('fmp')
        record['source'] = '+'.join(sources) if sources else 'unknown'
        
        return record
    
    def _parse_numeric(self, value) -> Optional[float]:
        """Parse numeric value from various string formats."""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            # Remove common prefixes/suffixes
            clean = value.replace('₹', '').replace('Cr', '').replace('%', '').strip()
            clean = clean.replace(',', '')
            
            try:
                return float(clean)
            except ValueError:
                return None
        
        return None
    
    # ============================================================
    # Utilities
    # ============================================================
    
    def get_table_stats(self, table_name: str = "bronze.ohlcv") -> Dict:
        """Get statistics for a bronze table."""
        try:
            table = self.catalog.load_table(table_name)
            if table is None:
                return {}
            
            # Get snapshot info
            snapshots = table.snapshots()
            
            return {
                'table': table_name,
                'snapshots': len(snapshots),
                'location': table.location,
            }
        except Exception as e:
            logger.error(f"Failed to get stats for {table_name}: {e}")
            return {}


# ============================================================
# CLI Interface
# ============================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion")
    parser.add_argument("--init", action="store_true", help="Initialize tables")
    parser.add_argument("--ohlcv", type=str, help="Ingest OHLCV for symbol")
    parser.add_argument("--ohlcv-all", action="store_true", help="Ingest all OHLCV")
    parser.add_argument("--fundamentals", type=str, help="Ingest fundamentals for symbol")
    parser.add_argument("--fundamentals-batch", nargs='+', help="Ingest fundamentals batch")
    parser.add_argument("--stats", action="store_true", help="Show table stats")
    
    args = parser.parse_args()
    
    ingestion = BronzeIngestion()
    
    if args.init:
        from lakehouse.iceberg_catalog import init_lakehouse_tables
        init_lakehouse_tables()
    
    elif args.ohlcv:
        count = ingestion.ingest_ohlcv_symbol(args.ohlcv.upper())
        print(f"Ingested {count} rows for {args.ohlcv.upper()}")
    
    elif args.ohlcv_all:
        results = ingestion.ingest_ohlcv_batch()
        total = sum(results.values())
        print(f"Ingested {total} total rows from {len(results)} symbols")
    
    elif args.fundamentals:
        success = ingestion.ingest_fundamentals(args.fundamentals.upper())
        print(f"Ingestion {'successful' if success else 'failed'} for {args.fundamentals.upper()}")
    
    elif args.fundamentals_batch:
        results = ingestion.ingest_fundamentals_batch([s.upper() for s in args.fundamentals_batch])
        print(f"Results: {results}")
    
    elif args.stats:
        stats = ingestion.get_table_stats("bronze.ohlcv")
        print(f"OHLCV Stats: {stats}")
        stats = ingestion.get_table_stats("bronze.fundamentals")
        print(f"Fundamentals Stats: {stats}")
    
    else:
        parser.print_help()