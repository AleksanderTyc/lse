# LSE DB Setup
# This is a DB setup script, intended to run as a one-off
# It creates empty DB structures.

# 03MAY2026 New fields added to symbols table,
#           related to yfinance data availability and sector trend'o'meter development

import sqlalchemy

from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, Integer, String, DateTime, Date
from sqlalchemy.sql import func

# Engine is created once for the entire process
engine = create_engine("sqlite+pysqlite:///../lse.db", echo=True)

# MetaData object is usually created once and used for the entire data schema of the application.
metadata_obj = MetaData()


# Events - records every load event, giving it unique Id and saving its timestamp.
events_table = Table(
    "load_events",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("load_timestamp", DateTime, nullable=False, default=func.datetime('subsec')),
    Column("load_type", String(1), nullable=False),
    Column("as_at_date", Date, nullable=False)
)


# Sectors - list of LSE sectors, unique id, sector name
sectors_table = Table(
    "sectors",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("sector_name", String(60), nullable=False)
)


# Tickers - list of ticker symbols by sector, unique id, sector_id, ticker, name.
# Ticker should be unique, if it is not, we have to investigate and clarify, arrive at some unique resolution.
symbols_table = Table(
    "symbols",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("sector_id", Integer, ForeignKey("sectors.id")),
    Column("symbol", String(10), nullable=False),
    Column("name", String),
    
# 03MAY2026 New fields added to symbols table
    Column("targetHighPrice", Integer),
    Column("targetLowPrice", Integer),
    Column("targetMeanPrice", Integer),
    Column("targetMedianPrice", Integer),
    Column("recommendationMean", Integer),
    Column("recommendationKey", String),
    Column("numberOfAnalystOpinions", Integer),
    Column("averageAnalystRating", String),
    Column("dividendRate", Integer),
    Column("dividendYield", Integer),
    Column("exDividendDate", Date),
    Column("payoutRatio", Integer),
    Column("fiveYearAvgDividendYield", Integer),
    Column("industryKey", String),
    Column("industryDisp", String),
    Column("sectorKey", String),
    Column("sectorDisp", String),
    Column("country", String),
    Column("marketCap", Integer),
    Column("shortName", String),
    Column("longName", String),
    Column("lastUpdateDate", Date, nullable=False),
    Column("attemptsSinceUpdate", Integer)
)


# Price performance - daily quote data available from Yahoo Finance about each ticker.
# Open, High, Low, Close should be recorded as Integer, basis points, i.e. price in pence *100
quotes_table = Table(
    "quotes",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("load_id", Integer, ForeignKey("load_events.id")),
    Column("quote_date", Date, nullable=False),
    Column("symbol_id", Integer, ForeignKey("symbols.id")),
    Column("symbol", String(10), nullable=False),
    Column("open", Integer),
    Column("high", Integer),
    Column("low", Integer),
    Column("close", Integer),
    Column("volume", Integer),
)


# Send MetaData to the engine to create the tables.
metadata_obj.create_all(engine)
