with running as (
    select
        ticker,
        trade_date,
        adj_close,
        max(adj_close) over (partition by ticker order by trade_date
                             rows between unbounded preceding and current row) as running_max
    from {{ ref('stg_prices_eod') }}
),

dd as (
    select *, adj_close / running_max - 1 as drawdown
    from running
)

select
    ticker,
    trade_date,
    adj_close,
    running_max,
    drawdown,
    min(drawdown) over (partition by ticker order by trade_date
                        rows between unbounded preceding and current row) as max_drawdown_to_date
from dd
