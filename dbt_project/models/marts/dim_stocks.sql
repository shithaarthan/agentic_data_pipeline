/*
Dimension table for stocks
Combines latest price data with fundamentals
*/

with latest_ohlcv as (
    select
        symbol,
        max(date) as latest_date
    from {{ ref('stg_ohlcv') }}
    group by symbol
),

latest_prices as (
    select
        o.symbol,
        o.close as latest_close,
        o.volume as latest_volume,
        o.daily_return_pct as latest_return,
        o.date as price_date
    from {{ ref('stg_ohlcv') }} o
    join latest_ohlcv l 
        on o.symbol = l.symbol 
        and o.date = l.latest_date
),

latest_indicators as (
    select
        symbol,
        sma_50,
        sma_200,
        rsi_14,
        trend_regime,
        market_condition,
        returns_20d
    from {{ ref('stg_indicators') }}
    where (symbol, date) in (
        select symbol, max(date)
        from {{ ref('stg_indicators') }}
        group by symbol
    )
),

fundamentals as (
    select
        symbol,
        market_cap,
        pe_ratio,
        pb_ratio,
        roe,
        debt_equity,
        revenue_growth,
        profit_growth,
        source
    from {{ source('bronze', 'fundamentals') }}
    where (symbol, date) in (
        select symbol, max(date)
        from {{ source('bronze', 'fundamentals') }}
        group by symbol
    )
),

combined as (
    select
        p.symbol,
        p.latest_close,
        p.latest_volume,
        p.latest_return,
        p.price_date,
        
        i.sma_50,
        i.sma_200,
        i.rsi_14,
        i.trend_regime,
        i.market_condition,
        i.returns_20d as momentum_20d,
        
        -- Price vs Moving Averages
        round((p.latest_close - i.sma_50) / i.sma_50 * 100, 2) as pct_from_sma50,
        round((p.latest_close - i.sma_200) / i.sma_200 * 100, 2) as pct_from_sma200,
        
        f.market_cap,
        f.pe_ratio,
        f.pb_ratio,
        f.roe,
        f.debt_equity,
        f.revenue_growth,
        f.profit_growth,
        f.source as fundamental_source,
        
        -- Value category
        case 
            when f.pe_ratio < 15 then 'VALUE'
            when f.pe_ratio > 40 then 'GROWTH'
            else 'BLEND'
        end as style_category,
        
        -- Composite health score (0-100)
        case
            when f.roe > 0.15 and f.debt_equity < 0.5 and f.pe_ratio < 25 then 90
            when f.roe > 0.10 and f.debt_equity < 1.0 then 70
            when f.roe > 0.05 then 50
            else 30
        end as fundamental_health_score,
        
        current_timestamp as dbt_updated_at
        
    from latest_prices p
    left join latest_indicators i on p.symbol = i.symbol
    left join fundamentals f on p.symbol = f.symbol
)

select *
from combined
order by fundamental_health_score desc, market_cap desc nulls last