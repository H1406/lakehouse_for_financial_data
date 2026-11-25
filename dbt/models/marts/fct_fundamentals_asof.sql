-- Each trading day joined to the latest fundamentals *filed on or before* that day (no look-ahead).
select
    p.ticker,
    p.trade_date,
    p.adj_close,
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
    f.eps_diluted
from {{ ref('stg_prices_eod') }} p
left join lateral (
    select fq.*
    from {{ ref('int_fundamentals_quarterly') }} fq
    where fq.ticker = p.ticker and fq.filed_date <= p.trade_date
    order by fq.filed_date desc, fq.period_end desc
    limit 1
) f on true
