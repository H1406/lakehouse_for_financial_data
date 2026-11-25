select
    ticker,
    minute,
    open,
    high,
    low,
    close,
    volume,
    published_at,
    extract(epoch from published_at - (minute + interval '1 minute')) as delay_seconds
from {{ source('lake_silver', 'equity_bars_1m') }}
