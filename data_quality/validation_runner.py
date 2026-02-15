"""
Data Quality Validation Runner
Integrates Great Expectations with the Foxa Lakehouse.
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from loguru import logger

# Add project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from lakehouse.iceberg_catalog import get_catalog


class DataQualityValidator:
    """
    Validates data quality using Great Expectations.
    Can run as part of the pipeline or standalone.
    """
    
    def __init__(self, context_root_dir: str = "great_expectations"):
        self.context_root_dir = Path(context_root_dir)
        self.catalog = get_catalog()
        
        # Defer GX import to avoid heavy dependency
        self._ge_context = None
    
    def _get_context(self):
        """Lazy load Great Expectations context."""
        if self._ge_context is None:
            try:
                import great_expectations as gx
                self._ge_context = gx.get_context(context_root_dir=str(self.context_root_dir))
                logger.info("Great Expectations context loaded")
            except ImportError:
                logger.error("Great Expectations not installed. Run: pip install great-expectations")
                raise
        return self._ge_context
    
    def validate_bronze_ohlcv(self) -> Dict:
        """
        Validate Bronze OHLCV data.
        
        Returns:
            Validation results dict
        """
        try:
            # Load data from Iceberg
            table = self.catalog.load_table("bronze.ohlcv")
            if table is None:
                return {'success': False, 'error': 'Table not found'}
            
            df = table.scan().to_pandas()
            
            if df.empty:
                return {'success': False, 'error': 'No data in table'}
            
            # Run validation (simplified without full GE for now)
            results = self._run_basic_validation(df, 'bronze_ohlcv')
            
            logger.info(f"Bronze OHLCV validation: {results['passed']}/{results['total']} checks passed")
            return results
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def validate_silver_indicators(self) -> Dict:
        """Validate Silver indicators data."""
        try:
            table = self.catalog.load_table("silver.indicators")
            if table is None:
                return {'success': False, 'error': 'Table not found'}
            
            df = table.scan().to_pandas()
            
            if df.empty:
                return {'success': False, 'error': 'No data in table'}
            
            results = self._run_indicator_validation(df)
            
            logger.info(f"Silver Indicators validation: {results['passed']}/{results['total']} checks passed")
            return results
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def _run_basic_validation(self, df: pd.DataFrame, table_name: str) -> Dict:
        """Run basic data quality checks."""
        checks = []
        
        # Check 1: Required columns exist
        required_cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        has_required = all(col in df.columns for col in required_cols)
        checks.append({
            'name': 'required_columns',
            'passed': has_required,
            'details': f"Required columns present: {has_required}"
        })
        
        # Check 2: No nulls in key columns
        null_counts = df[required_cols].isnull().sum()
        no_nulls = (null_counts == 0).all()
        checks.append({
            'name': 'no_nulls',
            'passed': no_nulls,
            'details': f"Null counts: {null_counts.to_dict()}"
        })
        
        # Check 3: Positive prices
        price_cols = ['open', 'high', 'low', 'close']
        positive_prices = (df[price_cols] > 0).all().all()
        checks.append({
            'name': 'positive_prices',
            'passed': positive_prices,
            'details': f"All prices positive: {positive_prices}"
        })
        
        # Check 4: OHLC logic (High >= Low, Open, Close)
        ohlc_logic = (
            (df['high'] >= df['low']) &
            (df['high'] >= df['open']) &
            (df['high'] >= df['close'])
        ).all()
        checks.append({
            'name': 'ohlc_logic',
            'passed': ohlc_logic,
            'details': f"OHLC logic valid: {ohlc_logic}"
        })
        
        # Check 5: Volume > 0
        positive_volume = (df['volume'] > 0).all()
        checks.append({
            'name': 'positive_volume',
            'passed': positive_volume,
            'details': f"All volumes positive: {positive_volume}"
        })
        
        # Check 6: Row count
        min_rows = 1000
        sufficient_rows = len(df) >= min_rows
        checks.append({
            'name': 'sufficient_rows',
            'passed': sufficient_rows,
            'details': f"Row count: {len(df)} (min: {min_rows})"
        })
        
        passed = sum(1 for c in checks if c['passed'])
        
        return {
            'table': table_name,
            'success': passed == len(checks),
            'passed': passed,
            'total': len(checks),
            'checks': checks,
            'row_count': len(df),
            'column_count': len(df.columns)
        }
    
    def _run_indicator_validation(self, df: pd.DataFrame) -> Dict:
        """Run validation on technical indicators."""
        checks = []
        
        # Check 1: RSI in valid range
        if 'rsi_14' in df.columns:
            rsi_valid = ((df['rsi_14'] >= 0) & (df['rsi_14'] <= 100)).all()
            checks.append({
                'name': 'rsi_range',
                'passed': rsi_valid,
                'details': f"RSI in range [0, 100]: {rsi_valid}"
            })
        
        # Check 2: ADX in valid range
        if 'adx_14' in df.columns:
            adx_valid = ((df['adx_14'] >= 0) & (df['adx_14'] <= 100)).all()
            checks.append({
                'name': 'adx_range',
                'passed': adx_valid,
                'details': f"ADX in range [0, 100]: {adx_valid}"
            })
        
        # Check 3: ATR positive
        if 'atr_14' in df.columns:
            atr_positive = (df['atr_14'] > 0).all()
            checks.append({
                'name': 'atr_positive',
                'passed': atr_positive,
                'details': f"ATR positive: {atr_positive}"
            })
        
        # Check 4: Trend regime valid values
        if 'trend_regime' in df.columns:
            valid_regimes = ['STRONG_UPTREND', 'UPTREND', 'SIDEWAYS', 'DOWNTREND', 'STRONG_DOWNTREND']
            regime_valid = df['trend_regime'].isin(valid_regimes).all()
            checks.append({
                'name': 'valid_trend_regimes',
                'passed': regime_valid,
                'details': f"All trend regimes valid: {regime_valid}"
            })
        
        # Check 5: SMA ordering (50 >= 200 in most cases)
        if 'sma_50' in df.columns and 'sma_200' in df.columns:
            # Just check they're comparable, not strictly ordered
            sma_comparable = (df['sma_50'] > 0).all() and (df['sma_200'] > 0).all()
            checks.append({
                'name': 'sma_positive',
                'passed': sma_comparable,
                'details': f"SMAs positive: {sma_comparable}"
            })
        
        passed = sum(1 for c in checks if c['passed'])
        
        return {
            'table': 'silver.indicators',
            'success': passed == len(checks),
            'passed': passed,
            'total': len(checks),
            'checks': checks,
            'row_count': len(df),
            'column_count': len(df.columns)
        }
    
    def run_all_validations(self) -> Dict[str, Dict]:
        """
        Run all data quality validations.
        
        Returns:
            Dict mapping table name to validation results
        """
        logger.info("Running all data quality validations...")
        
        results = {}
        results['bronze.ohlcv'] = self.validate_bronze_ohlcv()
        results['silver.indicators'] = self.validate_silver_indicators()
        
        total_passed = sum(r.get('passed', 0) for r in results.values())
        total_checks = sum(r.get('total', 0) for r in results.values())
        
        logger.success(f"All validations complete: {total_passed}/{total_checks} checks passed")
        
        return results


# ============================================================
# CLI Interface
# ============================================================

if __name__ == "__main__":
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Data Quality Validation")
    parser.add_argument("--bronze", action="store_true", help="Validate Bronze layer")
    parser.add_argument("--silver", action="store_true", help="Validate Silver layer")
    parser.add_argument("--all", action="store_true", help="Run all validations")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    args = parser.parse_args()
    
    validator = DataQualityValidator()
    
    if args.all:
        results = validator.run_all_validations()
    elif args.bronze:
        results = {'bronze.ohlcv': validator.validate_bronze_ohlcv()}
    elif args.silver:
        results = {'silver.indicators': validator.validate_silver_indicators()}
    else:
        parser.print_help()
        sys.exit(1)
    
    if args.json:
        print(json.dumps(results, indent=2, default=str))
    else:
        for table, result in results.items():
            print(f"\n{'='*60}")
            print(f"Table: {table}")
            print(f"{'='*60}")
            print(f"Success: {result.get('success', False)}")
            print(f"Passed: {result.get('passed', 0)}/{result.get('total', 0)}")
            print(f"Rows: {result.get('row_count', 0)}")
            
            if 'checks' in result:
                print("\nChecks:")
                for check in result['checks']:
                    status = "✓" if check['passed'] else "✗"
                    print(f"  {status} {check['name']}: {check['details']}")