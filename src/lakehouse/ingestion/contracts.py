"""Pydantic contracts for every external record. Built against tests/fixtures/ (recorded responses)."""

from datetime import date

from pydantic import BaseModel, ConfigDict, Field, model_validator

# yfinance daily history columns (Ticker.history(auto_adjust=False, actions=True)), index staged as "Date".
YAHOO_DAILY_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividends", "Stock Splits"]


class YahooDailyRow(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    trade_date: date = Field(alias="Date")
    open: float = Field(alias="Open", gt=0)
    high: float = Field(alias="High", gt=0)
    low: float = Field(alias="Low", gt=0)
    close: float = Field(alias="Close", gt=0)  # split-adjusted (yfinance), not the printed price
    adj_close: float = Field(alias="Adj Close", gt=0)  # split + dividend adjusted
    volume: int = Field(alias="Volume", ge=0)
    div_cash: float = Field(alias="Dividends", ge=0)
    split_factor: float = Field(alias="Stock Splits", ge=0)

    @model_validator(mode="after")
    def check(self) -> "YahooDailyRow":
        if self.high < self.low:
            raise ValueError("high < low")
        if self.split_factor == 0:  # yfinance reports 0.0 on days without a split
            self.split_factor = 1.0
        return self


class CompanyFactsEnvelope(BaseModel):
    """Top level of an EDGAR companyfacts payload. The nested facts are exploded in Spark, not here."""

    model_config = ConfigDict(extra="allow")

    cik: int
    entityName: str  # noqa: N815 - API field name
    facts: dict[str, dict]

    @model_validator(mode="after")
    def has_us_gaap(self) -> "CompanyFactsEnvelope":
        if "us-gaap" not in self.facts:
            raise ValueError("payload has no us-gaap facts")
        return self


class YahooMinuteBar(BaseModel):
    """Message on topic `equity_bars_1m`, built by streaming/equity_producer.py from yfinance 1m history.
    Spark uses the equivalent explicit schema in spark_jobs/stream_market.py."""

    ticker: str
    minute: str  # bar start, ISO 8601 with UTC offset
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None
    published_at: int  # ms: when the delayed feed released the bar
    delay_minutes: int


class BinanceTrade(BaseModel):
    """Inner `data` object of a Binance combined-stream trade event (fixture: binance_trades.jsonl).
    Spark uses the equivalent explicit schema in spark_jobs/stream_market.py."""

    e: str
    E: int  # event time, ms
    s: str  # symbol
    t: int  # trade id
    p: str  # price (decimal string)
    q: str  # quantity (decimal string)
    T: int  # trade time, ms
    m: bool  # is buyer the market maker
