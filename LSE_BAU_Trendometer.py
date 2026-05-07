# LSE_BAU_Trendometer
# Based on Wyzwanie Inwestycyjne:

# Paramaeter: Window: 130 bars

# Calculate 1. MA <- Moving Average (simple arithmetic) of the last Window bars, take Close price
# Calculate 2. RelS <- (C - MA)/MA
# Calculate 3. Strength <- RelS > 0 cast as integer: true: 1, false: 0
# Calculate 4. CountStrong <- sum(Strength) over all which have Volume > 0
# Calculate 5. CountTraded <- How many have Volume > 0
# Calculate 6. CountStrong[Sector] <- sum(Strength) over all which have Volume > 0 but limited to Sector
# Calculate 7. CountTraded[Sector] <- How many have Volume > 0 but limited to Sector

# We can now calculate and present indices:
# I1: CountStrong / CountTraded
# I2: CountStrong[Sector] / CountTraded[Sector]

"""
Plan

- load quote data into pd.dataframe
-- how much: we want to see progression of trendometer for the last 3 full weeks
-- take the most recent asatdate available, go back to Friday, go back 14+4 days, this should be Monday
-- go back 26 weeks. This will give us 130 bars 
- figure out the code to calculate MA over a window defined by (symbol) sorted by (date)
- calculate count of V > 0, overall and by sector
- MA: enrich with RelS, Strength, join to Sector to obtain Sector / Industry
- calculate count of Strength, overall and by sector

- now difficult part:
- present result as a table:
-- columns: sector, dates...
-- rows: overall, sectors...
-- cell: index of strong / count of V>0 for this sector (or overall) at this date
- present result as a graph:
-- choose sector / overall (drop down box)
-- present the graph of index over dates
"""


import datetime, sqlalchemy
import yfinance as yf
import pandas as pd
import numpy as np
import time as tm

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from typing import Tuple, List, Dict, Any, Optional
import sys

from LSE_1Off_DBSetup import engine, events_table


gl_data_start_date:datetime.date
gl_trend_start_date:datetime.date
gl_trend_end_date:datetime.date

gl_data:pd.DataFrame

# Determine applicable dates
def determine_dates(par_date:Optional[str]) -> Tuple[datetime.date,datetime.date,datetime.date]:
    """
    Determines data start date and trend start date

    The calculation start with as at date determination.
    As at date is the most recent quote date available in the DB.
    Alternatively, an optional parameter par_date may be provided, which overrides as at date.
    There is no check that par_date is available in DB.
    
    Trend end date is the most recent working date respective to (i.e. on or before) as at date.
        
    Trend start date is the date when trend calculation starts.
    Since we require progression of trend over the last 3 full weeks,
    we need to calculate the date when this period starts. Logic:
    - take as at date, go back to Friday (unless it is Friday), go back 14+4 days, this should be Monday.
    
    Data start date is the earliest quote date that must be loaded to correctly calculate trend.
    Trend is calculated from simple moving average over 130 bars. Logic:
    - take trend start date, go back 27 weeks. This will give us 130 bars.
    
    Args:
        par_date (str)(optional): as at date override

    Returns:    
        tuple[datetime.date,datetime.date]: ( data start date, trend start date, trend end date )
    """

    # Take as at date - either from the parameter or from the DB data
    if( par_date != None ):
        asatdate = datetime.date.fromisoformat(par_date)
    else:
        lc_max_id = 0
        lc_as_at_date = 0
        with engine.connect() as conn:
            result=conn.execute(sqlalchemy.text("select max(id) from load_events where load_type = 'P';"))
            lc_max_id = result.all()[0][0]
            print( f"* D * diag_only * lc_max_id is {lc_max_id}" )
            result=conn.execute(sqlalchemy.text("select as_at_date from load_events where id=:id;"), {'id':lc_max_id})
            lc_as_at_date = result.all()[0][0]
            print( f"* D * diag_only * lc_as_at_date is {lc_as_at_date}, {type(lc_as_at_date)}" )
        
        asatdate = datetime.date.fromisoformat(lc_as_at_date)
    
    day_of_week = asatdate.weekday()
    
    # Calculate the most recent working day respective to as at date
    r_trend_end = asatdate
    if( day_of_week > 4 ):
        r_trend_end = asatdate - datetime.timedelta(days=(day_of_week-4))
    
    # Calculate the most recent Friday respective to as at date
    days_to_subtract = (day_of_week + 3) % 7
    last_friday = asatdate - datetime.timedelta(days=days_to_subtract)
    
    # Go back to Monday 18 days before - this is trend start date
    r_trend_start = last_friday - datetime.timedelta(days=18)
    
    # Go back 27 weeks - this is data start date
    r_data_start = r_trend_start - datetime.timedelta(weeks=27)
    # r_data_start = r_trend_start - datetime.timedelta(weeks=2)
    
    return( r_data_start, r_trend_start , r_trend_end )


def extract_quote_data(par_date_start:datetime.date, par_date_end:datetime.date) -> pd.DataFrame:
    """
        Returns a dataframe for a given date interval.
        
        Extracts quote data (symbol, closing price) for all symbols available, for the given interval, eliminating symbols where volume is zero.
        
        Args:
            par_date_start (datetime.date): extraction interval start date
            par_date_end (datetime.date): extraction interval end date

        Returns:    
            pd.DataFrame: DataFrame ( quote date, symbol, symbol_id, close price )
    """
    
    # Build quotes query
    sql_quotes_query = sqlalchemy.text("""
        SELECT      quote_date,
                    symbol,
                    symbol_id,
                    close
        FROM        quotes
        WHERE           volume > 0
                    AND :start_date <= quote_date
                    AND quote_date <= :end_date
        ORDER BY    symbol,
                    quote_date
        ;
    """)

    # Define quotes query parameters        
    sql_quotes_params = {
        "start_date": par_date_start,
        "end_date": par_date_end,
        }
    
    # Build symbols query
    sql_symbols_query = sqlalchemy.text("""
        SELECT      symbol,
                    sectorKey,
                    industryKey,
                    marketCap
        FROM        symbols
        WHERE       attemptsSinceUpdate == 0
        ORDER BY    symbol
        ;
    """)

    # Define symbols query parameters        
    sql_symbols_params = {
        "start_date": par_date_start,
        "end_date": par_date_end,
        }

    # Load data into DataFrame
    with engine.connect() as conn:
        df = pd.read_sql_query( sql_quotes_query, conn, params=sql_quotes_params )
        # df_symbols = pd.read_sql_query(sql_symbols_query, conn, params=sql_symbols_params)
        df_symbols = pd.read_sql_query( sql_symbols_query, conn )
        df = df.merge(df_symbols, on='symbol')
    
    return df


def calculate_trend(
    par_granular: pd.DataFrame,
    par_sectorKey: Optional[str] = '<All>',
    par_industryKey: Optional[str] = '<All>'
    ) -> pd.DataFrame:
    """
        For the given granular data and sector / industry selection returns a dataframe with calculated trend.
        
        Subsets the granular data according to sector and industry parameters.
        Calculates MA (over a window of 130 bars) and determines if closing price is above the MA (symbol is strong).
        Calculates proportion of strong symbols on a given date.
        Calculates total market capitalisation for the current selection.
        
        Args:
            par_granular (pd.DataFrame): Granular data obtained from extract_quote_data
            par_sectorKey (str)(optional): sector filtering criterion, set as '<All>' for all-inclusive
            par_industryKey (str)(optional): industry filtering criterion, set as '<All>' for all-inclusive

        Returns:    
            pd.DataFrame: DataFrame ( quote date, symbol, symbol_id, close price )
    """
    
    # Subsets the granular data according to sector and industry parameters.
    dfcond = pd.Series(True, index = par_granular.index)
    if( par_sectorKey != '<All>'):
        dfcond = dfcond & (par_granular.sectorKey == par_sectorKey)
    if( par_industryKey != '<All>'):
        dfcond = dfcond & (par_granular.sectorKey == par_industryKey)
    dt_subset = par_granular[dfcond]

    # Calculates MA and determines if closing price is above the MA.
    # dt_aggd_s1 = dt_subset[['symbol','close']].groupby('symbol').rolling( window = 5, min_periods = 5 ).mean()
    dt_aggd_s1 = dt_subset[['symbol','close']].groupby('symbol').rolling( window = 130, min_periods = 130 ).mean()
    dt_aggd_s2 = dt_aggd_s1.reset_index(level='symbol')[['close']].rename(mapper = {'close':'ma'}, axis = 1)
    dt_aggd = pd.concat([dt_subset, dt_aggd_s2], axis=1) # yes, the index integrity is kept
    dt_aggd['strong'] = (dt_aggd.ma < dt_aggd.close).astype(int)
    # At this stage data are still at (date, symbol) level.

    # How many symbols are trading (vol>0) on this day, how many of them are strong
    # Count is simple - include all, because data have already been filtered to exclude trivial volume.
    # Also summarise total market capitalisation for the current selection.
    dt_aggd_s1 = dt_aggd[['strong','marketCap','quote_date']].groupby('quote_date').aggregate(['count', 'sum'])
    dt_aggd_s1.columns = pd.Index(['strong_count', 'strong_sum', 'mCap_count', 'mCap_sum'])
    # Trendometer is the proportion of strong among all contributing
    dt_aggd_s1['tmeter'] = dt_aggd_s1['strong_sum'] / dt_aggd_s1['strong_count']
    # Reindex by dates and sort.
    dt_aggd_s1.index = pd.to_datetime(dt_aggd_s1.index)
    dt_aggd_s1 = dt_aggd_s1.drop(['mCap_count'], axis=1).sort_index()
    return dt_aggd_s1.loc[gl_trend_start_date:gl_trend_end_date]


print( f'* D * Invoked as {sys.argv}, len of argv is {len(sys.argv)}' )
gl_data_start_date, gl_trend_start_date, gl_trend_end_date = determine_dates(None if len(sys.argv) == 1 else sys.argv[1])

print( f"* D * gl_data_start_date is {gl_data_start_date}" )
print( f"* D * gl_trend_start_date is {gl_trend_start_date}" )
print( f"* D * gl_trend_end_date is {gl_trend_end_date}" )

gl_data = extract_quote_data(gl_data_start_date, gl_trend_end_date)

# Are there any NaNs in 'close' column?
gl_data.close.isna().any()

# gl_trend_data = calculate_trend(gl_data, '<All>', '<All>')
gl_trend_data = calculate_trend(gl_data)
"""
gl_trend_data.loc[gl_trend_start_date:gl_trend_end_date]

gl_trend_data = calculate_trend(gl_data, 'technology')
gl_trend_data = calculate_trend(gl_data, 'technology', software-infrastructure')

"""

fig = go.Figure(data=[
    go.Bar(
        x=gl_trend_data.index,
        y=gl_trend_data['tmeter'],
        name='tmeter',
        marker_color='steelblue',
        marker_line_color='navy',
        marker_line_width=1,
        opacity=0.7,
        text=gl_trend_data['tmeter'].round(3),  # Show values on hover
        textposition='auto',
        hovertemplate='<b>Date</b>: %{x|%Y-%m-%d}<br>' +
                      '<b>tmeter</b>: %{y:.3f}<br>' +
                      '<extra></extra>'
    )
])

# Update layout
fig.update_layout(
    title='tmeter Values Over Time',
    xaxis_title='Date',
    yaxis_title='tmeter Value',
    yaxis=dict(
        range=[0, 1.05],
        tickformat='.0%',
        gridcolor='lightgray',
        gridwidth=0.5
    ),
    xaxis=dict(
        tickangle=45,
        tickformat='%Y-%m-%d'
    ),
    template='plotly_white',
    width=1200,
    height=600,
    hovermode='x unified'
)

# Show the chart
fig.show()

print("Interactive chart displayed! You can zoom, pan, and hover over bars.")

