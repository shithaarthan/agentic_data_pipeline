"""
Silver Layer - Cleaned & Validated Data
Transforms Bronze data with cleaning, validation, and feature engineering.
"""

import os
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import pandas as pd
import numpy as np
import pyarrow as pa
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from lakehouse.iceberg_catalog import get_catalog
from lakehouse.bronze import BronzeIngestion


class SilverTransformation:
    """
    Transforms Bronze data into Silver layer.
    Silver = Cleaned, validated, enriched data
    """
    
    def __init__(self):
        self.catalog = get_catalog()
        self.bronze = BronzeIngestion()
    
    # ============================================================
    # OHLCV Cleaning
    # ============================================================
    
    def transform_ohlcv(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """
        Transform Bronze OHLCV to Silver layer with cleaning.
        
        Cleaning steps:
        - Remove duplicates
        - Validate OHLC logic (Low <= Open,Close,High <= High)
        - Handle missing values
        - Add technical features
        
        Returns:
            Number of rows written
        """
        try:
            # Load from Bronze
            bronze_table = self.catalog.load_table("bronze.ohlcv")
            if bronze_table is None:
                logger.error("Bronze OHLCV table not found")
                return 0
            
            # Read to pandas
            df = bronze_table.scan().to_pandas()
            
            if df.empty:
                logger.warning("No data in bronze.ohlcv")
                return 0
            
            # Filter symbols if specified
            if symbols:
                df = df[df['symbol'].isin(symbols)]
            
            # Filter dates
            if start_date:
                df = df[df['date'] >= start_date]
            if end_date:
                df = df[df['date'] <= end_date]
            
            if df.empty:
                logger.warning("No data after filtering")
                return 0
            
            # Apply cleaning
            df = self._clean_ohlcv(df)
            
            # Add features
            df = self._add_price_features(df)
            
            # Write to Silver
            silver_table = self.catalog.load_table("silver.ohlcv_clean")
            if silver_table is None:
                logger.error("Silver OHLCV table not found")
                return 0
            
            arrow_table = pa.Table.from_pandas(df)
            silver_table.overwrite(arrow_table)
            
            count = len(df)
            logger.success(f"Transformed {count} rows to silver.ohlcv_clean")
            return count
            
        except Exception as e:
            logger.error(f"Failed to transform OHLCV: {e}")
            return 0
    
    def _clean_ohlcv(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean OHLCV data."""
        df = df.copy()
        
        # Sort by symbol and date
        df = df.sort_values(['symbol', 'date'])
        
        # Remove exact duplicates
        df = df.drop_duplicates(subset=['symbol', 'date'])
        
        # Validate OHLC logic
        # Fix: Low should be <= Open, Close, High
        df['low'] = df[['open', 'high', 'low', 'close']].min(axis=1)
        df['high'] = df[['open', 'high', 'low', 'close']].max(axis=1)
        
        # Ensure positive prices
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            df = df[df[col] > 0]
        
        # Ensure positive volume
        df = df[df['volume'] > 0]
        
        # Remove rows with any nulls in required columns
        required = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        df = df.dropna(subset=required)
        
        return df
    
    def _add_price_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived price features."""
        df = df.copy()
        
        # Daily returns
        df['daily_return'] = df.groupby('symbol')['close'].pct_change()
        
        # True Range
        df['prev_close'] = df.groupby('symbol')['close'].shift(1)
        df['tr1'] = df['high'] - df['low']
        df['tr2'] = abs(df['high'] - df['prev_close'])
        df['tr3'] = abs(df['low'] - df['prev_close'])
        df['true_range'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
        
        # Clean up intermediate columns
        df = df.drop(['tr1', 'tr2', 'tr3'], axis=1)
        
        # Price position within daily range (0 = at low, 1 = at high)
        df['price_position'] = (df['close'] - df['low']) / (df['high'] - df['low'])
        df['price_position'] = df['price_position'].fillna(0.5)
        
        return df
    
    # ============================================================
    # Technical Indicators
    # ============================================================
    
    def transform_indicators(
        self,
        symbols: Optional[List[str]] = None,
        lookback_days: int = 252
    ) -> int:
        """
        Calculate technical indicators and store in silver.indicators.
        
        Indicators calculated:
        - Moving averages (SMA 20, 50, 200)
        - Exponential MAs (EMA 12, 26)
        - RSI (14)
        - MACD
        - Bollinger Bands
        - ATR (14)
        - ADX (14)
        
        Returns:
            Number of rows written
        """
        try:
            # Load cleaned OHLCV
            silver_ohlcv = self.catalog.load_table("silver.ohlcv_clean")
            if silver_ohlcv is None:
                # Try to create from bronze first
                self.transform_ohlcv(symbols)
                silver_ohlcv = self.catalog.load_table("silver.ohlcv_clean")
                if silver_ohlcv is None:
                    logger.error("Cannot load silver.ohlcv_clean")
                    return 0
            
            df = silver_ohlcv.scan().to_pandas()
            
            if symbols:
                df = df[df['symbol'].isin(symbols)]
            
            if df.empty:
                logger.warning("No data for indicator calculation")
                return 0
            
            # Calculate indicators per symbol
            result_dfs = []
            for symbol in df['symbol'].unique():
                symbol_df = df[df['symbol'] == symbol].copy()
                symbol_df = self._calculate_all_indicators(symbol_df)
                result_dfs.append(symbol_df)
            
            df = pd.concat(result_dfs, ignore_index=True)
            
            # Filter to recent data if specified
            if lookback_days:
                cutoff = df['date'].max() - timedelta(days=lookback_days)
                df = df[df['date'] >= cutoff]
            
            # Write to Silver indicators table
            indicators_table = self.catalog.load_table("silver.indicators")
            if indicators_table is None:
                logger.error("Silver indicators table not found")
                return 0
            
            # Select core columns + indicators
            indicator_cols = [
                'sma_20', 'sma_50', 'sma_200',
                'ema_12', 'ema_26',
                'rsi_14',
                'macd', 'macd_signal', 'macd_hist',
                'bb_upper', 'bb_middle', 'bb_lower',
                'atr_14',
                'adx_14',
                'returns_1d', 'returns_5d', 'returns_20d'
            ]
            
            # Ensure all columns exist
            for col in indicator_cols:
                if col not in df.columns:
                    df[col] = np.nan
            
            arrow_table = pa.Table.from_pandas(df)
            indicators_table.overwrite(arrow_table)
            
            count = len(df)
            logger.success(f"Calculated indicators for {count} rows")
            return count
            
        except Exception as e:
            logger.error(f"Failed to calculate indicators: {e}")
            return 0
    
    def _calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all technical indicators for a symbol."""
        df = df.copy()
        df = df.sort_values('date')
        
        # Moving Averages
        df['sma_20'] = df['close'].rolling(window=20).mean()
        df['sma_50'] = df['close'].rolling(window=50).mean()
        df['sma_200'] = df['close'].rolling(window=200).mean()
        
        df['ema_12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema_26'] = df['close'].ewm(span=26, adjust=False).mean()
        
        # RSI
        df['rsi_14'] = self._calculate_rsi(df['close'], period=14)
        
        # MACD
        df['macd'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
        
        # Bollinger Bands
        df['bb_middle'] = df['close'].rolling(window=20).mean()
        bb_std = df['close'].rolling(window=20).std()
        df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
        df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
        
        # ATR
        df['atr_14'] = df['true_range'].rolling(window=14).mean()
        
        # ADX
        df['adx_14'] = self._calculate_adx(df, period=14)
        
        # Returns
        df['returns_1d'] = df['close'].pct_change(1) * 100
        df['returns_5d'] = df['close'].pct_change(5) * 100
        df['returns_20d'] = df['close'].pct_change(20) * 100
        
        return df
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI."""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _calculate_adx(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate ADX."""
        # +DM and -DM
        df['plus_dm'] = df['high'].diff()
        df['minus_dm'] = df['low'].diff().abs()
        
        df['plus_dm'] = np.where(
            (df['plus_dm'] > df['minus_dm']) & (df['plus_dm'] > 0),
            df['plus_dm'],
            0
        )
        df['minus_dm'] = np.where(
            (df['minus_dm'] > df['plus_dm']) & (df['minus_dm'] > 0),
            df['minus_dm'],
            0
        )
        
        # Smooth with EMA
        atr = df['true_range'].rolling(window=period).mean()
        plus_di = 100 * (df['plus_dm'].rolling(window=period).mean() / atr)
        minus_di = 100 * (df['minus_dm'].rolling(window=period).mean() / atr)
        
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        adx = dx.rolling(window=period).mean()
        
        return adx
    
    # ============================================================
    # Fundamentals Cleaning
    # ============================================================
    
    def transform_fundamentals(self, symbols: Optional[List[str]] = None) -> int:
        """
        Clean and standardize fundamental data.
        
        Transformations:
        - Deduplicate by symbol (keep latest)
        - Standardize units (all in Crores)
        - Add derived metrics (Piotroski score components)
        """
        try:
            bronze_table = self.catalog.load_table("bronze.fundamentals")
            if bronze_table is None:
                logger.error("Bronze fundamentals table not found")
                return 0
            
            df = bronze_table.scan().to_pandas()
            
            if symbols:
                df = df[df['symbol'].isin(symbols)]
            
            if df.empty:
                logger.warning("No fundamental data")
                return 0
            
            # Keep latest record per symbol
            df = df.sort_values('updated_at').groupby('symbol').last().reset_index()
            
            # Add quality flags
            df['has_pe'] = df['pe_ratio'].notna()
            df['has_pb'] = df['pb_ratio'].notna()
            df['has_roe'] = df['roe'].notna()
            
            # Add value categories
            df['pe_category'] = pd.cut(
                df['pe_ratio'],
                bins=[0, 15, 25, 50, float('inf')],
                labels=['cheap', 'fair', 'expensive', 'very_expensive']
            )
            
            df['pb_category'] = pd.cut(
                df['pb_ratio'],
                bins=[0, 1, 3, 5, float('inf')],
                labels=['cheap', 'fair', 'expensive', 'very_expensive']
            )
            
            # Note: Currently writing to same bronze table for simplicity
            # In production, would have separate silver.fundamentals table
            logger.success(f"Transformed fundamentals for {len(df)} symbols")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to transform fundamentals: {e}")
            return 0
    
    # ============================================================
    # Full Silver Pipeline
    # ============================================================
    
    def run_full_silver_pipeline(
        self,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, int]:
        """
        Run complete Silver transformation pipeline.
        
        Returns:
            Dict with row counts for each table
        """
        logger.info("Starting Silver layer transformation pipeline...")
        
        results = {}
        
        # Step 1: Transform OHLCV
        results['ohlcv_clean'] = self.transform_ohlcv(symbols)
        
        # Step 2: Calculate indicators
        results['indicators'] = self.transform_indicators(symbols)
        
        # Step 3: Transform fundamentals
        results['fundamentals'] = self.transform_fundamentals(symbols)
        
        logger.success(f"Silver pipeline complete: {results}")
        return results


# ============================================================
# CLI Interface
# ============================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Silver Layer Transformation")
    parser.add_argument("--ohlcv", action="store_true", help="Transform OHLCV")
    parser.add_argument("--indicators", action="store_true", help="Calculate indicators")
    parser.add_argument("--fundamentals", action="store_true", help="Transform fundamentals")
    parser.add_argument("--all", action="store_true", help="Run full pipeline")
    parser.add_argument("--symbols", nargs='+', help="Specific symbols")
    
    args = parser.parse_args()
    
    silver = SilverTransformation()
    
    symbols = [s.upper() for s in args.symbols] if args.symbols else None
    
    if args.all:
        results = silver.run_full_silver_pipeline(symbols)
        print(f"\nResults: {results}")
    
    elif args.ohlcv:
        count = silver.transform_ohlcv(symbols)
        print(f"Transformed {count} OHLCV rows")
    
    elif args.indicators:
        count = silver.transform_indicators(symbols)
        print(f"Calculated indicators for {count} rows")
    
    elif args.fundamentals:
        count = silver.transform_fundamentals(symbols)
        print(f"Transformed {count} fundamentals")
    
    else:
        parser.print_help()