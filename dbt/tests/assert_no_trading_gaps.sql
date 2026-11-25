{{ config(severity='warn') }}
-- Warn on gaps > 4 calendar days between consecutive trading days (a long weekend is at most 4).
select ticker, trade_date, prev_date, trade_date - prev_date as gap_days
from (
    select ticker, trade_date, lag(trade_date) over (partition by ticker order by trade_date) as prev_date
    from {{ ref('stg_prices_eod') }}
) t
where trade_date - prev_date > 4
