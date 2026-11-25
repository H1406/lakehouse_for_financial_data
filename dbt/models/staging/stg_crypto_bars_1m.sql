select
    symbol as ticker,
    minute,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    vwap
from {{ source('lake_silver', 'crypto_bars_1m') }}
