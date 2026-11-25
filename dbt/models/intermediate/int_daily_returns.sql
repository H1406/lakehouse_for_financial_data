-- Daily log returns on the split/dividend-adjusted close.
select
    ticker,
    trade_date,
    adj_close,
    ln(adj_close / lag(adj_close) over (partition by ticker order by trade_date)) as log_return
from {{ ref('stg_prices_eod') }}
