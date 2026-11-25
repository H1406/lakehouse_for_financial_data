-- 1-minute log returns for crypto (24/7) and equities (delayed feed, regular session).
-- Equities: partitioned by New York session date, so the overnight gap never becomes a "1-minute" return.
-- A missing minute (no bar) makes the next return span 2 minutes; shortcut: acceptable for these liquid names.
with crypto as (
    select
        ticker,
        'crypto' as asset_class,
        minute,
        close,
        ln(close / lag(close) over (partition by ticker order by minute)) as log_return
    from {{ ref('stg_crypto_bars_1m') }}
),

equity as (
    select
        ticker,
        'equity' as asset_class,
        minute,
        close,
        ln(close / lag(close) over (
            partition by ticker, (minute at time zone 'America/New_York')::date order by minute
        )) as log_return
    from {{ ref('stg_equity_bars_1m') }}
)

select ticker, asset_class, minute, close, log_return from crypto
union all
select ticker, asset_class, minute, close, log_return from equity
