-- One row per ticker x fiscal period: financials, changes, ratios and market risk over the period.
-- Feeds the agent fact sheets. QoQ compares with the previous reported quarter (Q1-Q3; Q4 is not reported
-- separately in XBRL, so Q1 compares with the prior Q3). YoY compares the same fiscal period a year earlier.
-- Risk columns cover the period: the last 91 days for a quarter, 365 days for FY.
with f as (
    select
        q.*,
        lag(q.revenue) over (partition by q.ticker, q.fiscal_period = 'FY' order by q.period_end) as revenue_prev_q,
        lag(q.revenue) over (partition by q.ticker, q.fiscal_period order by q.fiscal_year) as revenue_prev_y,
        lag(q.net_income) over (partition by q.ticker, q.fiscal_period order by q.fiscal_year) as net_income_prev_y
    from {{ ref('int_fundamentals_quarterly') }} q
),

risk as (
    select
        f.ticker,
        f.fiscal_year,
        f.fiscal_period,
        avg(v.vol_20d) as avg_vol_20d,
        max(v.vol_20d) as max_vol_20d,
        min(d.drawdown) as worst_drawdown,
        count(*) as trading_days
    from f
    join {{ ref('fct_rolling_volatility') }} v
      on v.ticker = f.ticker
     and v.trade_date > f.period_end - case when f.fiscal_period = 'FY' then 365 else 91 end
     and v.trade_date <= f.period_end
    join {{ ref('fct_drawdown') }} d on d.ticker = v.ticker and d.trade_date = v.trade_date
    group by f.ticker, f.fiscal_year, f.fiscal_period
)

select
    c.ticker,
    f.cik,
    c.name,
    c.sector,
    f.fiscal_year,
    f.fiscal_period,
    f.period_end,
    f.filed_date,
    f.accession_no,
    f.revenue,
    f.net_income,
    f.operating_income,
    f.total_assets,
    f.total_liabilities,
    f.stockholders_equity,
    f.eps_diluted,
    case when f.fiscal_period <> 'FY' and f.revenue_prev_q <> 0
         then f.revenue / f.revenue_prev_q - 1 end as revenue_qoq,
    case when f.revenue_prev_y <> 0 then f.revenue / f.revenue_prev_y - 1 end as revenue_yoy,
    case when f.net_income_prev_y <> 0 then f.net_income / abs(f.net_income_prev_y) - 1 end as net_income_yoy,
    case when f.revenue <> 0 then f.net_income / f.revenue end as net_margin,
    case when f.revenue <> 0 then f.operating_income / f.revenue end as operating_margin,
    case when f.stockholders_equity <> 0 then f.total_liabilities / f.stockholders_equity end as debt_to_equity,
    r.avg_vol_20d,
    r.max_vol_20d,
    r.worst_drawdown,
    r.trading_days,
    f.has_restatement
from f
join {{ ref('dim_company') }} c on c.ticker = f.ticker
left join risk r on r.ticker = f.ticker and r.fiscal_year = f.fiscal_year and r.fiscal_period = f.fiscal_period
