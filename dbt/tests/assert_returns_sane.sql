-- A daily log return beyond +/-0.5 (~ +65% / -39%) for a large cap almost always means a bad price or an
-- unadjusted split, not a real move.
select ticker, trade_date, log_return
from {{ ref('int_daily_returns') }}
where abs(log_return) > 0.5
