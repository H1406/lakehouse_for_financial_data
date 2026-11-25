-- Fundamentals may only be joined to a trading day on or after their filing date.
select ticker, trade_date, filed_date
from {{ ref('fct_fundamentals_asof') }}
where filed_date > trade_date
