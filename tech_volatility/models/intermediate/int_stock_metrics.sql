with stg_stocks as (
    -- Updated reference to your staging model
    -- Ensure your staging file is named 'stg_tech_stocks.sql' inside the staging folder
    select * from {{ ref('stg_tech_stocks') }}
),

metrics as (
    select
        stock_date,
        ticker,
        close_price,
        volume,
        
        -- Calculate Daily Return %: (Current - Prev) / Prev
        (close_price - lag(close_price) over (partition by ticker order by stock_date)) / 
        nullif(lag(close_price) over (partition by ticker order by stock_date), 0) as daily_return_pct,

        -- Calculate 7-Day Moving Average
        avg(close_price) over (
            partition by ticker 
            order by stock_date 
            rows between 6 preceding and current row
        ) as moving_avg_7d

    from stg_stocks
)

select * from metrics