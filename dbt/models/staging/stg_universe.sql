select
    ticker,
    nullif(cik, '') as cik,
    name,
    sector,
    asset_class,
    nullif(prior_ciks, '') as prior_ciks
from {{ ref('universe') }}
