/*
Market breadth metrics - daily aggregation
Tracks overall market health
*/

with daily_stats as (
    select
        date,
        
        -- Count of stocks by trend regime
        count(*) as total_stocks,
        count(case when trend_regime = 'STRONG_UPTREND' then 1 end) as strong_uptrend,
        count(case when trend_regime = 'UPTREND' then 1 end) as uptrend,
        count(case when trend_regime = 'SIDEWAYS' then 1 end) as sideways,
        count(case when trend_regime = 'DOWNTREND' then 1 end) as downtrend,
        count(case when trend_regime = 'STRONG_DOWNTREND' then 1 end) as strong_downtrend,
        
        -- RSI distribution
        count(case when rsi_signal = 'OVERBOUGHT' then 1 end) as overbought_count,
        count(case when rsi_signal = 'OVERSOLD' then 1 end) as oversold_count,
        
        -- MACD signals
        count(case when macd_signal_type = 'BULLISH_CROSSOVER' then 1 end) as macd_bullish_cross,
        count(case when macd_signal_type = 'BEARISH_CROSSOVER' then 1 end) as macd_bearish_cross,
        
        -- Average metrics
        round(avg(rsi_14), 2) as avg_rsi,
        round(avg(adx_14), 2) as avg_adx,
        round(avg(returns_20d), 2) as avg_momentum_20d,
        round(avg(daily_return_pct), 2) as avg_daily_return,
        
        -- Volume stats
        round(sum(volume_millions), 2) as total_volume_millions
        
    from {{ ref('stg_indicators') }}
    group by date
),

breadth_metrics as (
    select
        date,
        total_stocks,
        
        -- Trend breadth
        strong_uptrend,
        uptrend,
        sideways,
        downtrend,
        strong_downtrend,
        
        round((strong_uptrend + uptrend) * 100.0 / total_stocks, 2) as pct_in_uptrend,
        round((strong_downtrend + downtrend) * 100.0 / total_stocks, 2) as pct_in_downtrend,
        
        -- RSI breadth
        overbought_count,
        oversold_count,
        round(overbought_count * 100.0 / total_stocks, 2) as pct_overbought,
        round(oversold_count * 100.0 / total_stocks, 2) as pct_oversold,
        
        -- MACD breadth
        macd_bullish_cross,
        macd_bearish_cross,
        
        -- Averages
        avg_rsi,
        avg_adx,
        avg_momentum_20d,
        avg_daily_return,
        total_volume_millions,
        
        -- Market health score (0-100)
        round(
            (strong_uptrend * 5 + uptrend * 3 - downtrend * 2 - strong_downtrend * 5) 
            * 100.0 / (total_stocks * 5), 
            2
        ) as market_health_score,
        
        -- Market phase classification
        case
            when strong_uptrend > downtrend + strong_downtrend then 'BULLISH'
            when strong_downtrend > uptrend + strong_uptrend then 'BEARISH'
            when sideways > total_stocks * 0.4 then 'CONSOLIDATION'
            else 'MIXED'
        end as market_phase,
        
        current_timestamp as dbt_updated_at
        
    from daily_stats
)

select *
from breadth_metrics
order by date desc