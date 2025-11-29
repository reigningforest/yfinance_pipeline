with stock_metrics as (
    select * from {{ ref('int_stock_metrics') }}
)

select
    -- Create a surrogate key (unique ID) for testing
    (ticker || '-' || to_char(stock_date, 'YYYY-MM-DD')) as vol_id,
    
    stock_date,
    ticker,
    close_price,
    daily_return_pct,
    moving_avg_7d,
    volume,
    
    case 
        when abs(daily_return_pct) > 0.03 then true 
        else false 
    end as is_high_volatility

from stock_metrics
order by stock_date desc, ticker