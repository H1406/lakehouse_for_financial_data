-- One row per ticker x fiscal period (Q1, Q2, Q3, FY) with key metrics pivoted from XBRL concepts.
-- Grouped by ticker, not CIK: a company that re-registers (XOM, 2026) files under two CIKs over time.
-- A 10-Q also reports prior-year comparatives under the *filing's* fy/fp, so only each filing's current
-- period (its latest period_end) is kept. The latest filing per period wins (handles 10-Q/A restatements).
with facts as (
    select
        f.cik, f.ticker, f.accession_no, f.form, f.filed_date, f.fiscal_year, f.fiscal_period,
        f.period_start, f.period_end, f.duration_days, f.value,
        a.metric, a.priority, a.period_type
    from {{ ref('stg_financial_facts') }} f
    join {{ ref('concept_aliases') }} a
      on a.concept = f.concept and a.unit = f.unit and a.period_type = f.period_type
    where f.form in ('10-K', '10-Q', '10-K/A', '10-Q/A')
      and f.fiscal_period in ('Q1', 'Q2', 'Q3', 'FY')
),

with_filing_end as (
    select *, max(period_end) over (partition by cik, accession_no) as filing_period_end
    from facts
),

current_period as (
    -- the filing's own reporting period: latest period_end within each accession
    select * from with_filing_end where period_end = filing_period_end
),

sized as (
    -- duration facts must match the period length: ~1 quarter for Q1-Q3, ~1 year for FY (drops YTD values)
    select *
    from current_period
    where period_type = 'instant'
       or (fiscal_period <> 'FY' and duration_days between 80 and 100)
       or (fiscal_period = 'FY' and duration_days between 350 and 380)
),

ranked as (
    select
        *,
        row_number() over (
            partition by ticker, fiscal_year, fiscal_period, metric
            order by filed_date desc, priority, accession_no desc
        ) as rn,
        min(value) over w <> max(value) over w as values_differ
    from sized
    window w as (partition by ticker, fiscal_year, fiscal_period, metric)
),

chosen as (
    select * from ranked where rn = 1
)

select
    ticker,
    (array_agg(cik order by filed_date desc))[1] as cik,
    fiscal_year,
    fiscal_period,
    max(period_end) as period_end,
    max(filed_date) as filed_date,
    (array_agg(accession_no order by filed_date desc))[1] as accession_no,
    max(value) filter (where metric = 'revenue') as revenue,
    max(value) filter (where metric = 'net_income') as net_income,
    max(value) filter (where metric = 'operating_income') as operating_income,
    max(value) filter (where metric = 'total_assets') as total_assets,
    max(value) filter (where metric = 'total_liabilities') as total_liabilities,
    max(value) filter (where metric = 'stockholders_equity') as stockholders_equity,
    max(value) filter (where metric = 'eps_diluted') as eps_diluted,
    bool_or(values_differ) as has_restatement
from chosen
group by ticker, fiscal_year, fiscal_period
