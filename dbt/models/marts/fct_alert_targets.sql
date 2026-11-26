-- Subscribers to alert today: the latest 20d vol z-score of a followed ticker exceeds their threshold.
-- Keyed by subscriber_key only; contact details are resolved outside dbt by the pii_admin notifier.
select
    s.subscriber_key,
    s.ticker,
    v.trade_date,
    v.vol_20d_zscore,
    s.z_threshold
from {{ source('lake_silver', 'alert_subscriptions') }} s
join {{ ref('fct_rolling_volatility') }} v on v.ticker = s.ticker
where v.trade_date = (select max(trade_date) from {{ ref('fct_rolling_volatility') }})
  and v.vol_20d_zscore > s.z_threshold
