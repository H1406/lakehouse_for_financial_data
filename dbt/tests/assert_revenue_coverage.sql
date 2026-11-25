{{ config(severity='warn') }}
-- Each company should have revenue for >= 80% of its fiscal periods; lower means a missing concept alias.
select cik, ticker, count(*) as periods, count(revenue) as with_revenue
from {{ ref('int_fundamentals_quarterly') }}
group by cik, ticker
having count(revenue) < 0.8 * count(*)
