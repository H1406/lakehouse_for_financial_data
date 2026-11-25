-- Pairwise 60-trading-day rolling correlation of daily log returns (each unordered pair once).
with pairs as (
    select
        a.ticker as ticker_a,
        b.ticker as ticker_b,
        a.trade_date,
        a.log_return as return_a,
        b.log_return as return_b
    from {{ ref('int_daily_returns') }} a
    join {{ ref('int_daily_returns') }} b
      on a.trade_date = b.trade_date and a.ticker < b.ticker
    where a.log_return is not null and b.log_return is not null
)

select
    ticker_a,
    ticker_b,
    trade_date,
    case when count(*) over w = 60 then corr(return_a, return_b) over w end as corr_60d
from pairs
window w as (partition by ticker_a, ticker_b order by trade_date rows between 59 preceding and current row)
