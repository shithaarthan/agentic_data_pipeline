/*
Staging model for technical indicators
Transforms raw indicator data into analysis-ready format
*/

with source as (
    select *
    from {{ source('silver', 'indicators') }}
),

indicator_analysis as (
    select
        symbol,
        date,
        close,
        volume,
        
        -- Moving Averages
        sma_20,
        sma_50,
        sma_200,
        ema_12,
        ema_26,
        
        -- RSI interpretation
        rsi_14,
        case 
            when rsi_14 > 70 then 'OVERBOUGHT'
            when rsi_14 < 30 then 'OVERSOLD'
            else 'NEUTRAL'
        end as rsi_signal,
        
        -- MACD interpretation
        macd,
        macd_signal,
        macd_hist,
        case 
            when macd_hist > 0 and lag(macd_hist) over (partition by symbol order by date) <= 0 
                then 'BULLISH_CROSSOVER'
            when macd_hist < 0 and lag(macd_hist) over (partition by symbol order by date) >= 0 
                then 'BEARISH_CROSSOVER'
            when macd_hist > 0 then 'BULLISH'
            when macd_hist < 0 then 'BEARISH'
            else 'NEUTRAL'
        end as macd_signal_type,
        
        -- Bollinger Bands position
        bb_upper,
        bb_middle,
        bb_lower,
        case 
            when close > bb_upper then 'ABOVE_UPPER'
            when close < bb_lower then 'BELOW_LOWER'
            when close > bb_middle then 'UPPER_HALF'
            else 'LOWER_HALF'
        end as bb_position,
        
        -- Trend analysis
        case 
            when close > sma_20 and sma_20 > sma_50 and sma_50 > sma_200 
                then 'STRONG_UPTREND'
            when close > sma_50 and sma_50 > sma_200 
                then 'UPTREND'
            when close < sma_20 and sma_20 < sma_50 and sma_50 < sma_200 
                then 'STRONG_DOWNTREND'
            when close < sma_50 and sma_50 < sma_200 
                then 'DOWNTREND'
            else 'SIDEWAYS'
        end as trend_regime,
        
        -- ATR-based volatility
        atr_14,
        round(atr_14 / close * 100, 2) as atr_pct,
        
        -- ADX trend strength
        adx_14,
        case 
            when adx_14 > 25 then 'TRENDING'
            else 'RANGING'
        end as market_condition,
        
        -- Returns
        returns_1d,
        returns_5d,
        returns_20d,
        
        timestamp as loaded_at
        
    from source
)

select *
from indicator_analysis