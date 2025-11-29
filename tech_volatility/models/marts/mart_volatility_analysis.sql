with stock_metrics as (
    select * from {{ ref('int_stock_metrics') }}
)

select
    stock_date,
    ticker,
    close_price,
    daily_return_pct,
    moving_avg_7d,
    volume,
    
    -- Flag High Volatility Days (e.g., return > 3% or < -3%)
    case 
        when abs(daily_return_pct) > 0.03 then true 
        else false 
    end as is_high_volatility

from stock_metrics
order by stock_date desc, ticker