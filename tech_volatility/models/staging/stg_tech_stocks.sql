with source as (
    select * from {{ source('fivetran_data', 'tech_stocks_raw') }}
),

renamed as (
    select
        "DATE"::date as stock_date,
        "TICKER"::varchar as ticker,
        "OPEN"::float as open_price,
        "HIGH"::float as high_price,
        "LOW"::float as low_price,
        "CLOSE"::float as close_price,
        "VOLUME"::int as volume
    from source
)

select * from renamed
where stock_date is not null
  and close_price is not null -- FIX 1: Filter out rows with missing data
qualify row_number() over (partition by ticker, stock_date order by stock_date) = 1 -- FIX 2: Deduplicate (ensure only 1 row per ticker/date)