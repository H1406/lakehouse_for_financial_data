select
    ticker,
    cik,
    name,
    sector,
    asset_class
from {{ ref('stg_universe') }}
