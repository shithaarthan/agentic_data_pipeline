/*
Fact table for trading signals
Combines scanner signals with indicator context
*/

with signals as (
    select *
    from {{ source('gold', 'signals') }}
),

indicators as (
    select *
    from {{ ref('stg_indicators') }}
),

signals_with_context as (
    select
        s.symbol,
        s.date,
        s.strategy,
        s.signal,
        s.entry,
        s.stop,
        s.target,
        s.risk_reward,
        s.confidence as signal_confidence,
        
        -- Indicator context at signal time
        i.close as price_at_signal,
        i.rsi_14,
        i.rsi_signal,
        i.trend_regime,
        i.macd_signal_type,
        i.bb_position,
        i.market_condition,
        i.adx_14,
        i.returns_20d as momentum_20d,
        
        -- Calculate potential return
        round((s.target - s.entry) / s.entry * 100, 2) as potential_return_pct,
        
        -- Risk metrics
        round((s.entry - s.stop) / s.entry * 100, 2) as risk_pct,
        
        -- Signal quality score (composite)
        case 
            when s.confidence = 'HIGH' and i.rsi_signal != 'OVERBOUGHT' then 3
            when s.confidence = 'HIGH' then 2
            when s.confidence = 'MEDIUM' and i.trend_regime in ('STRONG_UPTREND', 'UPTREND') then 2
            when s.confidence = 'MEDIUM' then 1
            else 0
        end as quality_score,
        
        s.scan_timestamp
        
    from signals s
    left join indicators i 
        on s.symbol = i.symbol 
        and s.date = i.date
)

select *
from signals_with_context

{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}

order by date desc, quality_score desc, symbol