select
    ticker,
    trade_date,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    div_cash,
    split_factor,
    _updated_at
from {{ source('lake_silver', 'prices_eod') }}
