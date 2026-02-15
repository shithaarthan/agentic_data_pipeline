"""
Kimi Position Trading Scanner
Reads OHLCV from Parquet files, runs position trading strategies.
Strategies: Stage 2 Breakout, CANSLIM, Monthly Trend
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.ohlcv_fetcher import OHLCVStorage


# Output directory for signals
SIGNALS_DIR = Path(__file__).parent.parent / "data" / "signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)


class KimiScanner:
    """
    Position Trading Scanner - First Layer Filter
    Scans for 20-70% moves over 2-4 months.
    """
    
    def __init__(self):
        self.storage = OHLCVStorage()
        self.results: List[Dict] = []
    
    def load_ohlcv(self, symbol: str) -> Optional[pd.DataFrame]:
        """Load OHLCV data from parquet."""
        df = self.storage.load(symbol)
        if df is not None and len(df) >= 200:
            return df
        logger.warning(f"{symbol}: Insufficient data ({len(df) if df is not None else 0} days)")
        return None
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all technical indicators needed for strategies."""
        df = df.copy()
        
        # Moving Averages
        df['SMA_50'] = df['close'].rolling(window=50).mean()
        df['SMA_150'] = df['close'].rolling(window=150).mean()
        df['SMA_200'] = df['close'].rolling(window=200).mean()
        df['EMA_20'] = df['close'].ewm(span=20).mean()
        df['EMA_50'] = df['close'].ewm(span=50).mean()
        
        # Weekly MAs (using daily data)
        df['SMA_10w'] = df['close'].rolling(window=50).mean()   # ~10 weeks
        df['SMA_30w'] = df['close'].rolling(window=150).mean()  # ~30 weeks
        df['SMA_40w'] = df['close'].rolling(window=200).mean()  # ~40 weeks
        
        # RSI
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # Volume Analysis
        df['Vol_SMA_20'] = df['volume'].rolling(window=20).mean()
        df['Vol_Ratio'] = df['volume'] / df['Vol_SMA_20']
        df['Vol_SMA_50'] = df['volume'].rolling(window=50).mean()
        
        # ADX (Trend Strength)
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        
        plus_dm = df['high'].diff()
        minus_dm = df['low'].diff().abs()
        plus_dm = plus_dm.where(plus_dm > 0, 0)
        minus_dm = minus_dm.where(minus_dm > 0, 0)
        plus_dm = plus_dm.where(plus_dm > minus_dm, 0)
        minus_dm = minus_dm.where(minus_dm > plus_dm, 0)
        
        atr = tr.rolling(window=14).mean()
        plus_di = 100 * (plus_dm.rolling(window=14).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(window=14).mean() / atr)
        dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di + 0.0001)) * 100
        df['ADX'] = dx.rolling(window=14).mean()
        
        # Price Performance
        df['Returns_3m'] = df['close'].pct_change(periods=63) * 100
        df['Returns_6m'] = df['close'].pct_change(periods=126) * 100
        
        # 52-week range
        df['52w_High'] = df['close'].rolling(window=252).max()
        df['52w_Low'] = df['close'].rolling(window=252).min()
        df['Pct_from_52w_high'] = (df['close'] / df['52w_High'] - 1) * 100
        
        return df
    
    def detect_stage(self, df: pd.DataFrame) -> Dict:
        """
        Weinstein Stage Analysis:
        Stage 1: Basing (Accumulation)
        Stage 2: Advancing (Markup) - TARGET
        Stage 3: Distribution (Topping)
        Stage 4: Declining (Markdown)
        """
        current = df.iloc[-1]
        price = current['close']
        
        sma_30w = current['SMA_30w']
        sma_40w = current['SMA_40w']
        sma_50 = current['SMA_50']
        sma_150 = current['SMA_150']
        sma_200 = current['SMA_200']
        
        # Stage 2 Criteria
        is_stage_2 = (
            price > sma_30w and 
            price > sma_40w and
            sma_30w > sma_40w and
            price > sma_50 and
            price > sma_150 and
            price > sma_200 and
            sma_50 > sma_200
        )
        
        # Determine stage
        if is_stage_2:
            stage = 2
        elif price < sma_30w and price > sma_40w:
            stage = 1
        elif price < sma_30w and price < sma_40w:
            stage = 4
        else:
            stage = 3
        
        return {
            'is_stage_2': is_stage_2,
            'stage': stage
        }
    
    def strategy_stage2_breakout(self, df: pd.DataFrame, symbol: str) -> Optional[Dict]:
        """
        Strategy 1: Stage 2 Breakout
        Target: 20-50% moves over 2-4 months
        """
        current = df.iloc[-1]
        
        # Volume confirmation
        vol_confirmed = current['Vol_Ratio'] > 2.0
        
        # Near 52-week high
        near_high = current['Pct_from_52w_high'] > -10
        
        # Strong trend
        adx_strong = current['ADX'] > 25
        
        # Stage verification
        stage_info = self.detect_stage(df)
        
        # RSI not overbought
        rsi_ok = current['RSI'] < 70
        
        if (stage_info['is_stage_2'] and vol_confirmed and near_high and adx_strong and rsi_ok):
            entry_price = current['close']
            stop_loss = current['SMA_30w'] * 0.95
            target = entry_price * 1.40
            risk = (entry_price - stop_loss) / entry_price
            reward = 0.40
            
            return {
                'symbol': symbol,
                'strategy': 'stage2_breakout',
                'signal': 'BUY',
                'entry': round(entry_price, 2),
                'stop': round(stop_loss, 2),
                'target': round(target, 2),
                'risk_reward': round(reward / risk, 2) if risk > 0 else 0,
                'rsi': round(current['RSI'], 1),
                'adx': round(current['ADX'], 1),
                'vol_ratio': round(current['Vol_Ratio'], 1),
                'pct_from_high': round(current['Pct_from_52w_high'], 1),
                'stage': stage_info['stage'],
                'confidence': 'HIGH' if current['Vol_Ratio'] > 3 else 'MEDIUM'
            }
        return None
    
    def strategy_canslim(self, df: pd.DataFrame, symbol: str) -> Optional[Dict]:
        """
        Strategy 2: CANSLIM Simplified
        """
        current = df.iloc[-1]
        
        # N: New highs (within 5% of 52w high)
        new_highs = current['Pct_from_52w_high'] > -5
        
        # S & I: Volume interest
        volume_trend = df['Vol_Ratio'].tail(20).mean() > 1.2
        
        # L: Price momentum
        price_momentum = current['Returns_3m'] > 15
        
        # Pivot/breakout
        recent_high = df['close'].tail(20).max()
        recent_low = df['close'].tail(20).min()
        tight_range = (recent_high - recent_low) / df['close'].tail(20).mean() < 0.08
        breakout = current['close'] > recent_high * 0.98
        
        rsi_ok = current['RSI'] < 65
        
        if new_highs and volume_trend and price_momentum and (tight_range or breakout) and rsi_ok:
            entry = current['close']
            stop = current['SMA_50'] * 0.93
            target = entry * 1.35
            
            return {
                'symbol': symbol,
                'strategy': 'canslim',
                'signal': 'BUY',
                'entry': round(entry, 2),
                'stop': round(stop, 2),
                'target': round(target, 2),
                'returns_3m': round(current['Returns_3m'], 1),
                'rsi': round(current['RSI'], 1),
                'pct_from_high': round(current['Pct_from_52w_high'], 1),
                'vol_ratio': round(current['Vol_Ratio'], 1),
                'confidence': 'HIGH' if current['Vol_Ratio'] > 2.5 else 'MEDIUM'
            }
        return None
    
    def strategy_monthly_trend(self, df: pd.DataFrame, symbol: str) -> Optional[Dict]:
        """
        Strategy 3: Monthly Moving Average Stack
        Target: 40-70% multi-month trends
        """
        if len(df) < 252:
            return None
        
        current = df.iloc[-1]
        
        # MA alignment
        ma_aligned = (
            current['close'] > current['SMA_50'] > 
            current['SMA_200'] and
            df['SMA_200'].iloc[-1] > df['SMA_200'].iloc[-63]  # Rising 200 SMA
        )
        
        # Strong momentum
        momentum = current['Returns_6m'] > 25
        
        # Pullback to support
        near_support = abs(current['close'] / current['SMA_50'] - 1) < 0.03
        
        adx_ok = current['ADX'] > 20
        
        if ma_aligned and momentum and near_support and adx_ok:
            entry = current['close']
            stop = current['SMA_200']
            target = entry * 1.60
            risk_pct = (entry - stop) / entry
            
            return {
                'symbol': symbol,
                'strategy': 'monthly_trend',
                'signal': 'BUY',
                'entry': round(entry, 2),
                'stop': round(stop, 2),
                'target': round(target, 2),
                'risk_pct': f"{risk_pct:.1%}",
                'returns_6m': round(current['Returns_6m'], 1),
                'rsi': round(current['RSI'], 1),
                'adx': round(current['ADX'], 1),
                'confidence': 'MEDIUM'
            }
        return None
    
    def scan_symbol(self, symbol: str) -> List[Dict]:
        """Scan a single symbol with all strategies."""
        df = self.load_ohlcv(symbol)
        if df is None:
            return []
        
        df = self.calculate_indicators(df)
        signals = []
        
        # Run all strategies
        s1 = self.strategy_stage2_breakout(df, symbol)
        if s1:
            signals.append(s1)
        
        s2 = self.strategy_canslim(df, symbol)
        if s2:
            signals.append(s2)
        
        s3 = self.strategy_monthly_trend(df, symbol)
        if s3:
            signals.append(s3)
        
        return signals
    
    def scan_all(self, symbols: List[str] = None) -> Dict[str, Any]:
        """
        Scan all symbols and return signals.
        
        Args:
            symbols: List of symbols. If None, uses all stored parquets.
        
        Returns:
            Dict with scan results
        """
        if symbols is None:
            symbols = self.storage.list_symbols()
        
        if not symbols:
            logger.error("No symbols to scan. Run OHLCV fetcher first.")
            return {"signals": []}
        
        logger.info(f"Scanning {len(symbols)} stocks...")
        
        all_signals = []
        scanned = 0
        
        for symbol in symbols:
            try:
                signals = self.scan_symbol(symbol)
                all_signals.extend(signals)
                scanned += 1
                
                if signals:
                    for s in signals:
                        logger.info(f"✓ {symbol}: {s['strategy']} - {s['signal']}")
                else:
                    logger.debug(f"  {symbol}: No signals")
                    
            except Exception as e:
                logger.error(f"✗ {symbol}: {e}")
        
        # Sort by confidence
        confidence_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
        all_signals.sort(key=lambda x: (confidence_order.get(x.get('confidence', 'LOW'), 2), x['symbol']))
        
        result = {
            "scan_date": datetime.now().strftime("%Y-%m-%d"),
            "scan_time": datetime.now().strftime("%H:%M:%S"),
            "total_scanned": scanned,
            "total_signals": len(all_signals),
            "signals": all_signals
        }
        
        logger.success(f"Scan complete: {len(all_signals)} signals from {scanned} stocks")
        return result
    
    def save_signals(self, result: Dict, filename: str = None) -> Path:
        """Save signals to JSON file."""
        if filename is None:
            filename = f"kimi_{result['scan_date']}.json"
        
        filepath = SIGNALS_DIR / filename
        
        with open(filepath, 'w') as f:
            json.dump(result, f, indent=2)
        
        logger.success(f"Saved signals to {filepath}")
        return filepath
    
    def get_candidates_for_agents(self, result: Dict = None) -> List[str]:
        """Get list of symbols for agent analysis."""
        if result is None:
            # Load latest
            files = sorted(SIGNALS_DIR.glob("kimi_*.json"), reverse=True)
            if files:
                with open(files[0]) as f:
                    result = json.load(f)
            else:
                return []
        
        # Unique symbols with signals
        symbols = list(set(s['symbol'] for s in result.get('signals', [])))
        return symbols


# ============================================
# CLI
# ============================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Kimi Position Trading Scanner")
    parser.add_argument("--symbol", type=str, help="Scan single symbol")
    parser.add_argument("--save", action="store_true", help="Save results to file")
    
    args = parser.parse_args()
    
    scanner = KimiScanner()
    
    if args.symbol:
        signals = scanner.scan_symbol(args.symbol.upper())
        if signals:
            print(f"\n{args.symbol.upper()} Signals:")
            for s in signals:
                print(json.dumps(s, indent=2))
        else:
            print(f"No signals for {args.symbol.upper()}")
    else:
        result = scanner.scan_all()
        
        print("\n" + "=" * 60)
        print(f"KIMI SCAN RESULTS - {result['scan_date']}")
        print("=" * 60)
        print(f"Scanned: {result['total_scanned']} stocks")
        print(f"Signals: {result['total_signals']}")
        
        if result['signals']:
            print("\n" + "-" * 60)
            for s in result['signals']:
                print(f"  {s['symbol']:12} | {s['strategy']:18} | Entry: ₹{s['entry']:>8} | "
                      f"Target: ₹{s['target']:>8} | {s['confidence']}")
        else:
            print("\nNo signals found.")
        
        if args.save:
            scanner.save_signals(result)
        
        # Show candidates for agents
        candidates = scanner.get_candidates_for_agents(result)
        if candidates:
            print(f"\n→ Candidates for Agent analysis: {candidates}")
