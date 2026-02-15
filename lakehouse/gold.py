"""
Gold Layer - Analytics-Ready Data
Aggregated signals, portfolios, and business-level metrics.
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
from lakehouse.silver import SilverTransformation
from data.kimi_scanner import KimiScanner


class GoldAnalytics:
    """
    Creates analytics-ready Gold layer tables.
    Gold = Aggregated signals, portfolio metrics, business insights
    """
    
    def __init__(self):
        self.catalog = get_catalog()
        self.silver = SilverTransformation()
        self.scanner = KimiScanner()
    
    # ============================================================
    # Trading Signals
    # ============================================================
    
    def generate_signals(
        self,
        symbols: Optional[List[str]] = None,
        scan_date: Optional[date] = None
    ) -> int:
        """
        Generate trading signals and store in gold.signals.
        
        Combines:
        - Kimi scanner strategies (Stage 2, CANSLIM, Monthly Trend)
        - Technical indicator signals
        - Fundamentals overlay
        
        Returns:
            Number of signals generated
        """
        try:
            # Run Kimi scanner
            logger.info("Running Kimi scanner for signals...")
            scan_result = self.scanner.scan_all(symbols)
            
            signals = scan_result.get('signals', [])
            if not signals:
                logger.warning("No signals generated from scanner")
                return 0
            
            # Convert to DataFrame
            df = pd.DataFrame(signals)
            
            # Add metadata
            df['date'] = scan_date or date.today()
            df['scan_timestamp'] = datetime.now()
            
            # Ensure required columns
            required = ['symbol', 'date', 'strategy', 'signal', 'scan_timestamp']
            for col in required:
                if col not in df.columns:
                    df[col] = None
            
            # Write to Gold
            signals_table = self.catalog.load_table("gold.signals")
            if signals_table is None:
                logger.error("Gold signals table not found")
                return 0
            
            arrow_table = pa.Table.from_pandas(df)
            signals_table.overwrite(arrow_table)
            
            count = len(df)
            logger.success(f"Generated {count} trading signals")
            return count
            
        except Exception as e:
            logger.error(f"Failed to generate signals: {e}")
            return 0
    
    def get_latest_signals(
        self,
        min_confidence: str = "MEDIUM",
        strategy: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Retrieve latest signals with filtering.
        
        Args:
            min_confidence: Minimum confidence level (HIGH, MEDIUM, LOW)
            strategy: Filter by specific strategy
            
        Returns:
            DataFrame of signals
        """
        try:
            table = self.catalog.load_table("gold.signals")
            if table is None:
                return pd.DataFrame()
            
            df = table.scan().to_pandas()
            
            # Filter by confidence
            confidence_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
            min_level = confidence_order.get(min_confidence, 1)
            df = df[df['confidence'].map(confidence_order).fillna(2) <= min_level]
            
            # Filter by strategy
            if strategy:
                df = df[df['strategy'] == strategy]
            
            # Sort by date desc, then confidence
            df = df.sort_values(['date', 'confidence'], ascending=[False, True])
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get signals: {e}")
            return pd.DataFrame()
    
    # ============================================================
    # Agent Analysis Aggregation
    # ============================================================
    
    def aggregate_agent_analysis(
        self,
        analysis_results: List[Dict]
    ) -> int:
        """
        Aggregate AI agent analysis results into gold.agent_analysis.
        
        Args:
            analysis_results: List of analysis dicts with keys:
                - symbol, agent_type, recommendation, score, confidence, reasoning
                
        Returns:
            Number of records written
        """
        try:
            if not analysis_results:
                logger.warning("No analysis results to aggregate")
                return 0
            
            df = pd.DataFrame(analysis_results)
            
            # Add metadata
            df['date'] = date.today()
            df['analysis_timestamp'] = datetime.now()
            
            # Ensure required columns
            required = ['symbol', 'date', 'agent_type', 'recommendation', 'analysis_timestamp']
            for col in required:
                if col not in df.columns:
                    df[col] = None
            
            # Write to Gold
            table = self.catalog.load_table("gold.agent_analysis")
            if table is None:
                logger.error("Gold agent_analysis table not found")
                return 0
            
            arrow_table = pa.Table.from_pandas(df)
            table.overwrite(arrow_table)
            
            count = len(df)
            logger.success(f"Aggregated {count} agent analyses")
            return count
            
        except Exception as e:
            logger.error(f"Failed to aggregate agent analysis: {e}")
            return 0
    
    def get_consensus_recommendation(
        self,
        symbol: str,
        lookback_days: int = 7
    ) -> Dict:
        """
        Get consensus recommendation from all agents for a symbol.
        
        Returns:
            Dict with consensus, confidence, and breakdown
        """
        try:
            table = self.catalog.load_table("gold.agent_analysis")
            if table is None:
                return {}
            
            df = table.scan().to_pandas()
            
            # Filter by symbol and date
            df = df[df['symbol'] == symbol]
            cutoff = date.today() - timedelta(days=lookback_days)
            df = df[df['date'] >= cutoff]
            
            if df.empty:
                return {'symbol': symbol, 'consensus': 'NO_DATA'}
            
            # Count recommendations
            rec_counts = df['recommendation'].value_counts().to_dict()
            
            # Calculate weighted score
            score_map = {'STRONG_BUY': 2, 'BUY': 1, 'HOLD': 0, 'SELL': -1, 'STRONG_SELL': -2}
            df['score_num'] = df['recommendation'].map(score_map).fillna(0)
            df['weight'] = df['confidence'].fillna(50) / 100
            weighted_score = (df['score_num'] * df['weight']).sum() / df['weight'].sum()
            
            # Determine consensus
            if weighted_score >= 1.5:
                consensus = 'STRONG_BUY'
            elif weighted_score >= 0.5:
                consensus = 'BUY'
            elif weighted_score <= -1.5:
                consensus = 'STRONG_SELL'
            elif weighted_score <= -0.5:
                consensus = 'SELL'
            else:
                consensus = 'HOLD'
            
            return {
                'symbol': symbol,
                'consensus': consensus,
                'weighted_score': round(weighted_score, 2),
                'avg_confidence': round(df['confidence'].mean(), 1),
                'breakdown': rec_counts,
                'agent_count': len(df),
                'latest_date': df['date'].max().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get consensus for {symbol}: {e}")
            return {}
    
    # ============================================================
    # Portfolio Analytics
    # ============================================================
    
    def calculate_portfolio_metrics(
        self,
        positions: List[Dict]
    ) -> Dict:
        """
        Calculate portfolio-level metrics.
        
        Args:
            positions: List of position dicts with symbol, quantity, entry_price
            
        Returns:
            Portfolio metrics dict
        """
        try:
            if not positions:
                return {}
            
            df = pd.DataFrame(positions)
            
            # Get current prices from silver data
            ohlcv_table = self.catalog.load_table("silver.ohlcv_clean")
            if ohlcv_table:
                prices_df = ohlcv_table.scan().to_pandas()
                latest_prices = prices_df.groupby('symbol')['close'].last().to_dict()
                df['current_price'] = df['symbol'].map(latest_prices)
            else:
                df['current_price'] = df['entry_price']
            
            # Calculate metrics
            df['market_value'] = df['quantity'] * df['current_price']
            df['cost_basis'] = df['quantity'] * df['entry_price']
            df['unrealized_pnl'] = df['market_value'] - df['cost_basis']
            df['unrealized_pnl_pct'] = (df['unrealized_pnl'] / df['cost_basis']) * 100
            
            total_value = df['market_value'].sum()
            total_cost = df['cost_basis'].sum()
            
            metrics = {
                'total_market_value': round(total_value, 2),
                'total_cost_basis': round(total_cost, 2),
                'total_unrealized_pnl': round(df['unrealized_pnl'].sum(), 2),
                'total_return_pct': round(((total_value - total_cost) / total_cost) * 100, 2),
                'position_count': len(df),
                'winners': len(df[df['unrealized_pnl'] > 0]),
                'losers': len(df[df['unrealized_pnl'] < 0]),
                'avg_position_size': round(total_value / len(df), 2),
                'largest_position': df.loc[df['market_value'].idxmax(), 'symbol'] if not df.empty else None,
                'positions': df.to_dict('records')
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to calculate portfolio metrics: {e}")
            return {}
    
    # ============================================================
    # Market Analytics
    # ============================================================
    
    def generate_market_summary(self) -> Dict:
        """
        Generate daily market summary.
        
        Returns:
            Market summary dict
        """
        try:
            # Load signals
            signals_table = self.catalog.load_table("gold.signals")
            signals_df = signals_table.scan().to_pandas() if signals_table else pd.DataFrame()
            
            # Load silver OHLCV for market stats
            ohlcv_table = self.catalog.load_table("silver.ohlcv_clean")
            ohlcv_df = ohlcv_table.scan().to_pandas() if ohlcv_table else pd.DataFrame()
            
            summary = {
                'date': date.today().isoformat(),
                'generated_at': datetime.now().isoformat()
            }
            
            # Signal summary
            if not signals_df.empty:
                today_signals = signals_df[signals_df['date'] == date.today()]
                summary['signals_today'] = len(today_signals)
                summary['signals_by_strategy'] = today_signals['strategy'].value_counts().to_dict()
                summary['signals_by_confidence'] = today_signals['confidence'].value_counts().to_dict()
            else:
                summary['signals_today'] = 0
            
            # Market breadth
            if not ohlcv_df.empty:
                latest = ohlcv_df.groupby('symbol').last()
                prev = ohlcv_df.groupby('symbol').nth(-2)
                
                advancers = len(latest[latest['close'] > prev['close']])
                decliners = len(latest[latest['close'] < prev['close']])
                unchanged = len(latest) - advancers - decliners
                
                summary['market_breadth'] = {
                    'advancers': advancers,
                    'decliners': decliners,
                    'unchanged': unchanged,
                    'advance_decline_ratio': round(advancers / max(decliners, 1), 2)
                }
                
                # Average volume
                summary['avg_volume'] = round(latest['volume'].mean(), 0)
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to generate market summary: {e}")
            return {}
    
    # ============================================================
    # Full Gold Pipeline
    # ============================================================
    
    def run_full_gold_pipeline(
        self,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, any]:
        """
        Run complete Gold analytics pipeline.
        
        Returns:
            Dict with pipeline results
        """
        logger.info("Starting Gold layer analytics pipeline...")
        
        results = {}
        
        # Step 1: Generate signals
        results['signals_generated'] = self.generate_signals(symbols)
        
        # Step 2: Generate market summary
        results['market_summary'] = self.generate_market_summary()
        
        logger.success(f"Gold pipeline complete")
        return results


# ============================================================
# CLI Interface
# ============================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Gold Layer Analytics")
    parser.add_argument("--signals", action="store_true", help="Generate signals")
    parser.add_argument("--summary", action="store_true", help="Generate market summary")
    parser.add_argument("--all", action="store_true", help="Run full pipeline")
    parser.add_argument("--symbols", nargs='+', help="Specific symbols")
    parser.add_argument("--get-signals", action="store_true", help="Show latest signals")
    parser.add_argument("--consensus", type=str, help="Get consensus for symbol")
    
    args = parser.parse_args()
    
    gold = GoldAnalytics()
    
    symbols = [s.upper() for s in args.symbols] if args.symbols else None
    
    if args.all:
        results = gold.run_full_gold_pipeline(symbols)
        print(f"\nResults: {results}")
    
    elif args.signals:
        count = gold.generate_signals(symbols)
        print(f"Generated {count} signals")
    
    elif args.summary:
        summary = gold.generate_market_summary()
        print(f"\nMarket Summary: {summary}")
    
    elif args.get_signals:
        signals = gold.get_latest_signals()
        print(f"\nLatest Signals:\n{signals}")
    
    elif args.consensus:
        consensus = gold.get_consensus_recommendation(args.consensus.upper())
        print(f"\nConsensus for {args.consensus.upper()}: {consensus}")
    
    else:
        parser.print_help()