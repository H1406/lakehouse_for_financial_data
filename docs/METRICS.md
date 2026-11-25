# Metric definitions

Single source of truth for the marts. If a model's logic changes, update this file in the same commit.

| Metric | Definition | Model |
|---|---|---|
| Daily log return | `ln(adj_close_t / adj_close_{t-1})` on yfinance's split- and dividend-adjusted close, per ticker, consecutive trading days | `int_daily_returns` |
| Annualized volatility (N d) | sample stdev of the last N daily log returns × √252. Null until N returns exist. N = 20, 60 | `fct_rolling_volatility` |
| Vol z-score | `(vol_20d − mean(vol_20d)) / stdev(vol_20d)` over the trailing 252 trading days (incl. today); null with < 126 values | `fct_rolling_volatility` |
| Drawdown | `adj_close / running_max(adj_close) − 1` (0 at a new high, negative below) | `fct_drawdown` |
| Max drawdown to date | minimum drawdown from the first trading day in range up to t | `fct_drawdown` |
| 60d correlation | Pearson correlation of two tickers' daily log returns over the last 60 common trading days | `fct_correlation_60d` |
| Intraday volatility | sample stdev of the last 60 one-minute log returns. Crypto × √525,600 (24/7). Equities × √98,280 (390 min × 252 days), with the window reset each New York session. Equity bars come from the delayed (≥ 10 min) feed | `fct_intraday_volatility` |
| Feed delay | `published_at − (minute + 1 min)` for an equity bar: ≥ 600 s live, larger for replayed history | `stg_equity_bars_1m.delay_seconds` |
| Point-in-time fundamentals | the latest fiscal period whose `filed_date <= trade_date` (no look-ahead) | `fct_fundamentals_asof` |
| Revenue YoY | `revenue / revenue(same fiscal period, previous fiscal year) − 1` | `mart_fact_sheet_inputs` |
| Revenue QoQ | `revenue / revenue(previous reported quarter) − 1`; Q1–Q3 only (Q4 is not reported separately in XBRL) | `mart_fact_sheet_inputs` |
| Net / operating margin | `net_income / revenue`, `operating_income / revenue` | `mart_fact_sheet_inputs` |
| Debt to equity | `total_liabilities / stockholders_equity` | `mart_fact_sheet_inputs` |
| Period risk | avg/max `vol_20d` and worst drawdown over the last 91 days (quarter) or 365 days (FY) up to `period_end` | `mart_fact_sheet_inputs` |

Concept → metric mapping lives in `dbt/seeds/concept_aliases.csv` (lowest `priority` wins per period).
