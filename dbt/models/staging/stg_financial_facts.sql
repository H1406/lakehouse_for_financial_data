select
    cik,
    ticker,
    concept,
    unit,
    period_start,
    period_end,
    value,
    accession_no,
    fy as fiscal_year,
    fp as fiscal_period,
    form,
    filed_date,
    frame,
    case when period_start is null then 'instant' else 'duration' end as period_type,
    (period_end - period_start) as duration_days,
    _updated_at
from {{ source('lake_silver', 'financial_facts') }}
