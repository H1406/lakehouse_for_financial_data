-- 60-minute rolling volatility of 1-minute log returns, annualized per market:
-- crypto trades 24/7 (525,600 minutes/year); US equities 390 minutes x 252 days (98,280 minutes/year).
-- Equity windows reset each New York session so they never straddle the overnight gap.
with windowed as (
    select
        ticker,
        asset_class,
        minute,
        close,
        log_return,
        count(log_return) over w as n_returns,
        stddev_samp(log_return) over w as stdev_60m
    from {{ ref('int_intraday_returns_1m') }}
    window w as (
        partition by ticker,
                     case when asset_class = 'equity' then (minute at time zone 'America/New_York')::date end
        order by minute rows between 59 preceding and current row
    )
)

select
    ticker,
    asset_class,
    minute,
    close,
    log_return,
    case when n_returns = 60 then
        case when asset_class = 'crypto' then {{ annualize_vol('stdev_60m', 525600) }}
             else {{ annualize_vol('stdev_60m', 98280) }} end
    end as vol_60m
from windowed
