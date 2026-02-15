/*
Staging model for OHLCV data
Cleans and standardizes raw market data from Silver layer
*/

with source as (
    select *
    from {{ source('silver', 'ohlcv_clean') }}
),

renamed as (
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        exchange,
        
        -- Calculate daily metrics
        round((close - open) / open * 100, 2) as daily_return_pct,
        round((high - low) / low * 100, 2) as daily_range_pct,
        round(volume / 1000000.0, 2) as volume_millions,
        
        -- Price position within day's range (0-1)
        case 
            when high = low then 0.5
            else round((close - low) / (high - low), 4)
        end as close_position_in_range,
        
        -- True Range
        round(
            greatest(
                high - low,
                abs(high - lag(close) over (partition by symbol order by date)),
                abs(low - lag(close) over (partition by symbol order by date))
            ), 2
        ) as true_range,
        
        -- Is gap up/down
        case 
            when open > lag(high) over (partition by symbol order by date) then 'GAP_UP'
            when open < lag(low) over (partition by symbol order by date) then 'GAP_DOWN'
            else 'NO_GAP'
        end as gap_type,
        
        timestamp as loaded_at
        
    from source
),

filtered as (
    select *
    from renamed
    where 
        -- Data quality filters
        open > 0 
        and high > 0 
        and low > 0 
        and close > 0
        and volume > 0
        and high >= low
        and high >= close
        and high >= open
        and low <= close
        and low <= open
)

select *
from filtered