{{ config(
    materialized='incremental',
    unique_key=['ticker', 'trade_date'],
    incremental_strategy='merge',
    on_schema_change='fail'
) }}
{#
  Incremental with two lookbacks:
  - output: rows from the last 90 days are rewritten each run (late corrections, MERGE upsert).
  - input: windows need history *before* those rows. The z-score compares vol_20d with its trailing
    252 trading days, and vol_20d itself needs 20 returns: ~272 trading days ~ 395 calendar days,
    plus the 90-day output window -> read 500 calendar days.
#}
{# coalesce: if the existing table is empty, max() is null and `>= null` would match nothing, forever. #}
{% set output_lookback_days = 90 %}
{% set input_lookback_days = 500 %}

with returns as (
    select ticker, trade_date, log_return
    from {{ ref('int_daily_returns') }}
    {% if is_incremental() %}
    where trade_date >= (select coalesce(max(trade_date), date '1900-01-01') - {{ input_lookback_days }} from {{ this }})
    {% endif %}
),

vol as (
    select
        ticker,
        trade_date,
        log_return,
        case when count(log_return) over w20 = 20
             then {{ annualize_vol('stddev_samp(log_return) over w20') }} end as vol_20d,
        case when count(log_return) over w60 = 60
             then {{ annualize_vol('stddev_samp(log_return) over w60') }} end as vol_60d
    from returns
    window
        w20 as (partition by ticker order by trade_date rows between 19 preceding and current row),
        w60 as (partition by ticker order by trade_date rows between 59 preceding and current row)
),

zscore as (
    select
        *,
        avg(vol_20d) over w252 as vol_20d_mean_1y,
        stddev_samp(vol_20d) over w252 as vol_20d_std_1y,
        count(vol_20d) over w252 as n_vol_1y
    from vol
    window w252 as (partition by ticker order by trade_date rows between 251 preceding and current row)
)

select
    ticker,
    trade_date,
    log_return,
    vol_20d,
    vol_60d,
    case when n_vol_1y >= 126 and vol_20d_std_1y > 0
         then (vol_20d - vol_20d_mean_1y) / vol_20d_std_1y end as vol_20d_zscore
from zscore
{% if is_incremental() %}
where trade_date >= (select coalesce(max(trade_date), date '1900-01-01') - {{ output_lookback_days }} from {{ this }})
{% endif %}
