"""Technical Analysis module with indicators and signal generation."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum
import pandas as pd
import numpy as np
from loguru import logger

try:
    import pandas_ta as ta
    HAS_PANDAS_TA = True
except ImportError:
    HAS_PANDAS_TA = False
    logger.warning("pandas_ta not installed, using basic implementations")


class Signal(Enum):
    """Trading signal types."""
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    NEUTRAL = "neutral"
    SELL = "sell"
    STRONG_SELL = "strong_sell"


@dataclass
class IndicatorResult:
    """Result from a technical indicator."""
    name: str
    value: float
    signal: Signal
    description: str


@dataclass
class TechnicalSummary:
    """Summary of all technical analysis."""
    symbol: str
    current_price: float
    indicators: Dict[str, IndicatorResult] = field(default_factory=dict)
    overall_signal: Signal = Signal.NEUTRAL
    bullish_count: int = 0
    bearish_count: int = 0
    neutral_count: int = 0
    analysis_text: str = ""


class TechnicalAnalyzer:
    """
    Technical analysis engine for stock data.
    Calculates various indicators and generates trading signals.
    """
    
    def __init__(self, df: pd.DataFrame):
        """
        Initialize with OHLCV DataFrame.
        
        Args:
            df: DataFrame with columns: timestamp, open, high, low, close, volume
        """
        self.df = df.copy()
        self._ensure_columns()
        self._calculate_indicators()
    
    def _ensure_columns(self):
        """Ensure required columns exist."""
        required = ["open", "high", "low", "close", "volume"]
        for col in required:
            if col not in self.df.columns:
                raise ValueError(f"Missing required column: {col}")
        
        # Convert to float
        for col in ["open", "high", "low", "close"]:
            self.df[col] = self.df[col].astype(float)
        self.df["volume"] = self.df["volume"].astype(int)
    
    def _calculate_indicators(self):
        """Calculate all technical indicators."""
        close = self.df["close"]
        high = self.df["high"]
        low = self.df["low"]
        volume = self.df["volume"]
        
        # === Moving Averages ===
        self.df["sma_20"] = close.rolling(window=20).mean()
        self.df["sma_50"] = close.rolling(window=50).mean()
        self.df["sma_200"] = close.rolling(window=200).mean()
        self.df["ema_9"] = close.ewm(span=9, adjust=False).mean()
        self.df["ema_21"] = close.ewm(span=21, adjust=False).mean()
        
        # === RSI (Relative Strength Index) ===
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        self.df["rsi"] = 100 - (100 / (1 + rs))
        
        # === MACD ===
        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        self.df["macd"] = exp1 - exp2
        self.df["macd_signal"] = self.df["macd"].ewm(span=9, adjust=False).mean()
        self.df["macd_histogram"] = self.df["macd"] - self.df["macd_signal"]
        
        # === Bollinger Bands ===
        self.df["bb_middle"] = close.rolling(window=20).mean()
        bb_std = close.rolling(window=20).std()
        self.df["bb_upper"] = self.df["bb_middle"] + (bb_std * 2)
        self.df["bb_lower"] = self.df["bb_middle"] - (bb_std * 2)
        self.df["bb_width"] = (self.df["bb_upper"] - self.df["bb_lower"]) / self.df["bb_middle"]
        
        # === Stochastic Oscillator ===
        low_14 = low.rolling(window=14).min()
        high_14 = high.rolling(window=14).max()
        self.df["stoch_k"] = 100 * (close - low_14) / (high_14 - low_14)
        self.df["stoch_d"] = self.df["stoch_k"].rolling(window=3).mean()
        
        # === Average True Range (ATR) ===
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        self.df["atr"] = tr.rolling(window=14).mean()
        
        # === On-Balance Volume (OBV) ===
        obv = [0]
        for i in range(1, len(close)):
            if close.iloc[i] > close.iloc[i-1]:
                obv.append(obv[-1] + volume.iloc[i])
            elif close.iloc[i] < close.iloc[i-1]:
                obv.append(obv[-1] - volume.iloc[i])
            else:
                obv.append(obv[-1])
        self.df["obv"] = obv
        
        # === Volume SMA ===
        self.df["volume_sma"] = volume.rolling(window=20).mean()
        
        # === ADX (Average Directional Index) ===
        self._calculate_adx()
        
        # === Momentum ===
        self.df["momentum"] = close - close.shift(10)
        self.df["roc"] = ((close - close.shift(10)) / close.shift(10)) * 100  # Rate of Change
    
    def _calculate_adx(self):
        """Calculate ADX (Average Directional Index)."""
        high = self.df["high"]
        low = self.df["low"]
        close = self.df["close"]
        
        plus_dm = high.diff()
        minus_dm = low.diff()
        
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm > 0] = 0
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        atr = tr.rolling(window=14).mean()
        plus_di = 100 * (plus_dm.rolling(window=14).mean() / atr)
        minus_di = abs(100 * (minus_dm.rolling(window=14).mean() / atr))
        
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        self.df["adx"] = dx.rolling(window=14).mean()
        self.df["plus_di"] = plus_di
        self.df["minus_di"] = minus_di
    
    def get_latest(self) -> Dict[str, float]:
        """Get latest values of all indicators."""
        latest = self.df.iloc[-1]
        return {
            col: latest[col] 
            for col in self.df.columns 
            if col not in ["timestamp", "open", "high", "low", "close", "volume"]
        }
    
    def analyze_rsi(self) -> IndicatorResult:
        """Analyze RSI for signals."""
        rsi = self.df["rsi"].iloc[-1]
        
        if pd.isna(rsi):
            return IndicatorResult("RSI", 0, Signal.NEUTRAL, "Insufficient data")
        
        if rsi < 20:
            signal = Signal.STRONG_BUY
            desc = f"RSI at {rsi:.1f} - Extremely oversold, potential reversal"
        elif rsi < 30:
            signal = Signal.BUY
            desc = f"RSI at {rsi:.1f} - Oversold condition"
        elif rsi > 80:
            signal = Signal.STRONG_SELL
            desc = f"RSI at {rsi:.1f} - Extremely overbought, potential reversal"
        elif rsi > 70:
            signal = Signal.SELL
            desc = f"RSI at {rsi:.1f} - Overbought condition"
        else:
            signal = Signal.NEUTRAL
            desc = f"RSI at {rsi:.1f} - Neutral zone"
        
        return IndicatorResult("RSI", round(rsi, 2), signal, desc)
    
    def analyze_macd(self) -> IndicatorResult:
        """Analyze MACD for signals."""
        macd = self.df["macd"].iloc[-1]
        signal_line = self.df["macd_signal"].iloc[-1]
        histogram = self.df["macd_histogram"].iloc[-1]
        prev_histogram = self.df["macd_histogram"].iloc[-2] if len(self.df) > 1 else 0
        
        if pd.isna(macd) or pd.isna(signal_line):
            return IndicatorResult("MACD", 0, Signal.NEUTRAL, "Insufficient data")
        
        # Bullish crossover
        if histogram > 0 and prev_histogram <= 0:
            signal = Signal.STRONG_BUY
            desc = "MACD bullish crossover - Strong buy signal"
        # Bearish crossover
        elif histogram < 0 and prev_histogram >= 0:
            signal = Signal.STRONG_SELL
            desc = "MACD bearish crossover - Strong sell signal"
        # Above signal line
        elif macd > signal_line:
            signal = Signal.BUY
            desc = "MACD above signal line - Bullish momentum"
        # Below signal line
        elif macd < signal_line:
            signal = Signal.SELL
            desc = "MACD below signal line - Bearish momentum"
        else:
            signal = Signal.NEUTRAL
            desc = "MACD neutral"
        
        return IndicatorResult("MACD", round(histogram, 2), signal, desc)
    
    def analyze_moving_averages(self) -> IndicatorResult:
        """Analyze moving average crossovers."""
        close = self.df["close"].iloc[-1]
        ema_9 = self.df["ema_9"].iloc[-1]
        ema_21 = self.df["ema_21"].iloc[-1]
        sma_50 = self.df["sma_50"].iloc[-1]
        sma_200 = self.df["sma_200"].iloc[-1]
        
        if pd.isna(sma_50):
            return IndicatorResult("Moving Averages", 0, Signal.NEUTRAL, "Insufficient data")
        
        signals = []
        
        # EMA crossover
        if ema_9 > ema_21:
            signals.append(1)  # Bullish
        else:
            signals.append(-1)  # Bearish
        
        # Price vs 50 SMA
        if close > sma_50:
            signals.append(1)
        else:
            signals.append(-1)
        
        # Golden/Death cross (50 vs 200)
        if not pd.isna(sma_200):
            if sma_50 > sma_200:
                signals.append(1)  # Golden cross
            else:
                signals.append(-1)  # Death cross
        
        avg_signal = sum(signals) / len(signals)
        
        if avg_signal > 0.5:
            signal = Signal.BUY
            desc = "Moving averages bullish - Price above key MAs"
        elif avg_signal < -0.5:
            signal = Signal.SELL
            desc = "Moving averages bearish - Price below key MAs"
        else:
            signal = Signal.NEUTRAL
            desc = "Mixed moving average signals"
        
        return IndicatorResult("Moving Averages", round(avg_signal, 2), signal, desc)
    
    def analyze_bollinger_bands(self) -> IndicatorResult:
        """Analyze Bollinger Bands for signals."""
        close = self.df["close"].iloc[-1]
        bb_upper = self.df["bb_upper"].iloc[-1]
        bb_lower = self.df["bb_lower"].iloc[-1]
        bb_middle = self.df["bb_middle"].iloc[-1]
        
        if pd.isna(bb_upper):
            return IndicatorResult("Bollinger Bands", 0, Signal.NEUTRAL, "Insufficient data")
        
        # Calculate position within bands (0 = lower, 1 = upper)
        bb_position = (close - bb_lower) / (bb_upper - bb_lower)
        
        if close < bb_lower:
            signal = Signal.STRONG_BUY
            desc = f"Price below lower band - Potentially oversold"
        elif close > bb_upper:
            signal = Signal.STRONG_SELL
            desc = f"Price above upper band - Potentially overbought"
        elif bb_position < 0.2:
            signal = Signal.BUY
            desc = "Price near lower band - Potential bounce"
        elif bb_position > 0.8:
            signal = Signal.SELL
            desc = "Price near upper band - Potential resistance"
        else:
            signal = Signal.NEUTRAL
            desc = "Price within bands - No extreme condition"
        
        return IndicatorResult("Bollinger Bands", round(bb_position, 2), signal, desc)
    
    def analyze_stochastic(self) -> IndicatorResult:
        """Analyze Stochastic Oscillator."""
        stoch_k = self.df["stoch_k"].iloc[-1]
        stoch_d = self.df["stoch_d"].iloc[-1]
        
        if pd.isna(stoch_k):
            return IndicatorResult("Stochastic", 0, Signal.NEUTRAL, "Insufficient data")
        
        if stoch_k < 20 and stoch_d < 20:
            signal = Signal.STRONG_BUY
            desc = f"Stochastic at {stoch_k:.1f} - Oversold"
        elif stoch_k < 30:
            signal = Signal.BUY
            desc = f"Stochastic at {stoch_k:.1f} - Approaching oversold"
        elif stoch_k > 80 and stoch_d > 80:
            signal = Signal.STRONG_SELL
            desc = f"Stochastic at {stoch_k:.1f} - Overbought"
        elif stoch_k > 70:
            signal = Signal.SELL
            desc = f"Stochastic at {stoch_k:.1f} - Approaching overbought"
        else:
            signal = Signal.NEUTRAL
            desc = f"Stochastic at {stoch_k:.1f} - Neutral"
        
        return IndicatorResult("Stochastic", round(stoch_k, 2), signal, desc)
    
    def analyze_adx(self) -> IndicatorResult:
        """Analyze ADX for trend strength."""
        adx = self.df["adx"].iloc[-1]
        plus_di = self.df["plus_di"].iloc[-1]
        minus_di = self.df["minus_di"].iloc[-1]
        
        if pd.isna(adx):
            return IndicatorResult("ADX", 0, Signal.NEUTRAL, "Insufficient data")
        
        trend_strength = "weak" if adx < 20 else "moderate" if adx < 40 else "strong"
        
        if adx > 25:
            if plus_di > minus_di:
                signal = Signal.BUY
                desc = f"ADX {adx:.1f} - {trend_strength.title()} uptrend"
            else:
                signal = Signal.SELL
                desc = f"ADX {adx:.1f} - {trend_strength.title()} downtrend"
        else:
            signal = Signal.NEUTRAL
            desc = f"ADX {adx:.1f} - Weak/no trend"
        
        return IndicatorResult("ADX", round(adx, 2), signal, desc)
    
    def analyze_volume(self) -> IndicatorResult:
        """Analyze volume patterns."""
        volume = self.df["volume"].iloc[-1]
        volume_sma = self.df["volume_sma"].iloc[-1]
        close = self.df["close"].iloc[-1]
        prev_close = self.df["close"].iloc[-2] if len(self.df) > 1 else close
        
        if pd.isna(volume_sma):
            return IndicatorResult("Volume", 0, Signal.NEUTRAL, "Insufficient data")
        
        volume_ratio = volume / volume_sma
        price_change = close - prev_close
        
        if volume_ratio > 1.5:
            if price_change > 0:
                signal = Signal.STRONG_BUY
                desc = f"High volume ({volume_ratio:.1f}x avg) with price increase"
            else:
                signal = Signal.STRONG_SELL
                desc = f"High volume ({volume_ratio:.1f}x avg) with price decrease"
        elif volume_ratio > 1.2:
            if price_change > 0:
                signal = Signal.BUY
                desc = f"Above average volume with bullish price action"
            else:
                signal = Signal.SELL
                desc = f"Above average volume with bearish price action"
        else:
            signal = Signal.NEUTRAL
            desc = f"Normal volume ({volume_ratio:.1f}x avg)"
        
        return IndicatorResult("Volume", round(volume_ratio, 2), signal, desc)
    
    def get_full_analysis(self, symbol: str = "UNKNOWN") -> TechnicalSummary:
        """
        Perform complete technical analysis.
        
        Returns:
            TechnicalSummary with all indicators and overall signal
        """
        current_price = self.df["close"].iloc[-1]
        
        # Get all indicator analyses
        indicators = {
            "rsi": self.analyze_rsi(),
            "macd": self.analyze_macd(),
            "moving_averages": self.analyze_moving_averages(),
            "bollinger_bands": self.analyze_bollinger_bands(),
            "stochastic": self.analyze_stochastic(),
            "adx": self.analyze_adx(),
            "volume": self.analyze_volume(),
        }
        
        # Count signals
        bullish = sum(1 for i in indicators.values() 
                     if i.signal in [Signal.BUY, Signal.STRONG_BUY])
        bearish = sum(1 for i in indicators.values() 
                     if i.signal in [Signal.SELL, Signal.STRONG_SELL])
        neutral = len(indicators) - bullish - bearish
        
        # Determine overall signal
        if bullish >= 5:
            overall = Signal.STRONG_BUY
        elif bullish >= 4:
            overall = Signal.BUY
        elif bearish >= 5:
            overall = Signal.STRONG_SELL
        elif bearish >= 4:
            overall = Signal.SELL
        else:
            overall = Signal.NEUTRAL
        
        # Generate analysis text
        analysis_lines = [
            f"📊 Technical Analysis for {symbol}",
            f"Current Price: ₹{current_price:,.2f}",
            f"",
            f"Signal Summary: {bullish} Bullish | {bearish} Bearish | {neutral} Neutral",
            f"Overall: {overall.value.upper().replace('_', ' ')}",
            f"",
            "Indicator Details:"
        ]
        
        for name, result in indicators.items():
            emoji = "🟢" if result.signal in [Signal.BUY, Signal.STRONG_BUY] else \
                    "🔴" if result.signal in [Signal.SELL, Signal.STRONG_SELL] else "⚪"
            analysis_lines.append(f"  {emoji} {result.name}: {result.description}")
        
        return TechnicalSummary(
            symbol=symbol,
            current_price=current_price,
            indicators=indicators,
            overall_signal=overall,
            bullish_count=bullish,
            bearish_count=bearish,
            neutral_count=neutral,
            analysis_text="\n".join(analysis_lines)
        )
    
    def get_support_resistance(self, lookback: int = 20) -> Dict[str, List[float]]:
        """Calculate support and resistance levels."""
        recent = self.df.tail(lookback)
        
        # Find local minima for support
        supports = []
        resistances = []
        
        for i in range(2, len(recent) - 2):
            low = recent["low"].iloc[i]
            high = recent["high"].iloc[i]
            
            # Check for local minimum
            if low < recent["low"].iloc[i-1] and low < recent["low"].iloc[i-2] and \
               low < recent["low"].iloc[i+1] and low < recent["low"].iloc[i+2]:
                supports.append(low)
            
            # Check for local maximum
            if high > recent["high"].iloc[i-1] and high > recent["high"].iloc[i-2] and \
               high > recent["high"].iloc[i+1] and high > recent["high"].iloc[i+2]:
                resistances.append(high)
        
        return {
            "support": sorted(set(round(s, 2) for s in supports))[-3:] if supports else [],
            "resistance": sorted(set(round(r, 2) for r in resistances))[:3] if resistances else []
        }


# Usage example
if __name__ == "__main__":
    from data.market_data import MarketData
    
    # Get historical data
    md = MarketData()
    df = md.get_historical("RELIANCE", days=100)
    
    if df is not None:
        # Analyze
        analyzer = TechnicalAnalyzer(df)
        summary = analyzer.get_full_analysis("RELIANCE")
        
        print(summary.analysis_text)
        print("\n" + "="*50)
        print("\nSupport/Resistance Levels:")
        levels = analyzer.get_support_resistance()
        print(f"  Support: {levels['support']}")
        print(f"  Resistance: {levels['resistance']}")
