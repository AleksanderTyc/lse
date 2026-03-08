# LSE Symbols selection

# from LSE_1Off_DBSetup import engine, events_table, sectors_table, symbols_table, quotes_table
from LSE_1Off_DBSetup import engine, events_table

import datetime, sqlalchemy
import yfinance as yf
import pandas as pd

from typing import Tuple, List, Dict, Any, Optional

# gl_load_id:Optional[int] = None
# gl_as_at_date:Optional[datetime.date] = None
# gl_start_date:Optional[datetime.date] = None

gl_load_id:int
gl_as_at_date:datetime.date
gl_start_date:datetime.date

# Determine current dates
def determine_dates(par_date:Optional[str]) -> Tuple[datetime.date,datetime.date]:
    """
    Determines start date and as at date of the current load.
    
    By default, as at date is the last working day before the current date.
    Optionally it accepts a date as string formatted YYYY-MM-DD which becomes as at date.
    By logic, start date is as at date minus 104 weeks and then back to previous Monday.
    
    Args:
        par_date (str)(optional): as at date override
        
    Returns:
        tuple[datetime.date,datetime.date]: ( start date, as at date )
    """
    
    if( par_date != None ):
        asatdate = datetime.date.fromisoformat(par_date)
    else:
        current_date = datetime.datetime.now().date()
        day_of_week = current_date.weekday()
        # If Tuesday (1) through Friday (4), subtract 1 day
        if 1 <= day_of_week <= 4:
            asatdate = current_date - datetime.timedelta(days=1)
        else:
            # For Saturday (5), Sunday (6), Monday (0), go to previous Friday
            # Days to subtract: Saturday=1, Sunday=2, Monday=3
            days_to_subtract = (day_of_week + 2) % 7 + 1
            asatdate = current_date - datetime.timedelta(days=days_to_subtract)
        
    startdate = asatdate - datetime.timedelta(days=asatdate.weekday()) -datetime.timedelta(weeks=104)
    # startdate = asatdate - datetime.timedelta(days=asatdate.weekday()) -datetime.timedelta(weeks=4)

    return( startdate, asatdate )



def create_load_record(par_date:datetime.date) -> int:
    """
    Create a new load record in events_table.
    Use par_date provided as as_at_date of the load.
    Default load_type to 'I' for 'Initial'.
    Obtain and return load_id.
    
    Args:
        par_date (datetime.date): as at date of the load
        
    Returns:
        int: load id
    """

    with engine.connect() as conn:
        sql_insert_expr = sqlalchemy.insert(events_table).values(load_type='I', as_at_date=par_date)
        # print( f"* D * sql_insert_expr {sql_insert_expr}" )
        result = conn.execute(sql_insert_expr)
        # print( f"* D * inserted_primary_key {result.inserted_primary_key[0]}" )
        conn.commit()
        return result.inserted_primary_key[0]


# Diagnostics
# print( f"param_load_id is {param_load_id}" )

# as_at_str = asatdate.strftime("%Y-%m-%d")
# start_str = startdate.strftime("%Y-%m-%d")

# print( f"as_at_str is {as_at_str}" )
# print( f"start_str is {start_str}" )


# Create symbols_to_process list, mapping symbol, symbol.L and symbol_id
def get_processed_symbols() -> Tuple[List[str],Dict[str,int],Dict[str,str]]:
    """
    Get symbols to process list from DB.
    Convert symbols to .L format required by YF to avoid ambiguity.
    Build and return two dictionaries to be used to map symbol column later:
    - .L symbol to symbol id
    - .L symbol to original symbol
    
    Returns:
        List[str]: Entire list of symbol.L
        Tuple[Dict[str,int],Dict[str,str]]: (
            {symbol.L -> symbol id},
            {symbol.L -> symbol}
        )
    """
    
    symbols_to_process = []
    map_yfsymbol_to_id:Dict[str,int] = {}
    map_yfsymbol_to_symbol:Dict[str,str] = {}
    with engine.connect() as conn:
        result=conn.execute(sqlalchemy.text("select id, symbol from symbols;"))
        for row in result:
            yf_symbol = '.'.join([row[1], 'L'])
            symbols_to_process.append(yf_symbol)
            map_yfsymbol_to_id[yf_symbol] = row[0]
            map_yfsymbol_to_symbol[yf_symbol] = row[1]
            
    return( symbols_to_process, map_yfsymbol_to_id, map_yfsymbol_to_symbol )


# Diagnostics, prototyping
# print( f"* D * sybols_to_process * len is {len(symbols_to_process)}, at index 0 is {symbols_to_process[0]}" )
# cur_symbols_to_process = symbols_to_process[0:31]
# cur_symbols_to_process = symbols_to_process[0:3]
# print( f"* D * cur_symbols_to_process * len is {len(cur_symbols_to_process)}, at index 1 is {cur_symbols_to_process[1]}" )


# Download data from YF, apply filtering using symbols_to_process and dates.
# Notice that this process may need to be split into slices to avoid excessive data volume.
# df = yf.download([smb['symbol'] for smb in cur_symbols_to_process], start='2026-02-16', end='2026-02-20', interval='1d', actions=True)
# df = yf.download([smb['yf_symbol'] for smb in cur_symbols_to_process], start=start_str, end=as_at_str, interval='1d', actions=True)
# print( f"* D * df.axes * {df.axes}" )
# print( f"* D * df.index * {df.index}" )

# Now we will do in a loop:
# - take another slice of symbols
# - download data from YF
# - reformat data, populate additional columns
# - open a new transaction on DB
# - delete quote data pertinent to the current slice
# - insert current slice data
# - commit the transaction
# We have 1488 symbols, i.e. 31*48. We will process 48 slices, 31 symbols each.

def take_nth_slice(par_slice_no:int) -> pd.DataFrame:
    """
    Import n-th slice of symbols data from YF.
    Reshape the dataframe to desired format.
    Return reshaped dataframe.
    
    Args:
        par_slice_no (int): slice index, in range(0,48)
        
    Returns:
        pd.DataFrame: reshaped dataframe
    """
    
    # Take current slice of symbols
    slice_symbols = symbols_to_process[par_slice_no*31:(par_slice_no+1)*31]
    
    # Download data from YF, apply filtering using symbols_to_process and dates.
    df = yf.download(
        slice_symbols,
        start=gl_start_date.strftime("%Y-%m-%d"),
        end=gl_as_at_date.strftime("%Y-%m-%d"),
        interval='1d',
        actions=True
        )

    # Reformat data, populate additional columns
    df_long = df.stack(level='Ticker', future_stack=True).rename(columns={'Volume':'volume'}).reset_index()

    df_long['load_id'] = gl_load_id
    df_long['quote_date']=(df_long['Date']).dt.date
    df_long['symbol'] = df_long['Ticker'].map(lambda x: map_yfsymbol_to_symbol[x])
    df_long['symbol_id'] = df_long['Ticker'].map(lambda x: map_yfsymbol_to_id[x])

    # Convert price columns from float to integer (multiply by 1,000,000)
    df_long['open'] = (df_long['Open']*1000000).round().astype(pd.Int64Dtype())
    df_long['high'] = (df_long['High']*1000000).round().astype(pd.Int64Dtype())
    df_long['low'] = (df_long['Low']*1000000).round().astype(pd.Int64Dtype())
    df_long['close'] = (df_long['Close']*1000000).round().astype(pd.Int64Dtype())
    
    return df_long


def save_nth_slice(par_slice_no:int, par_slice:pd.DataFrame):
    """
    Save dataframe provided into DB quotes table.
    
    Args:
        par_slice_no (int): slice index, in range(0,48)
        par_slice (pd.DataFrame): Pandas DataFrame to be saved to quotes
    """
    
    # Open a new transaction on DB
    with engine.connect() as conn:
    # Delete quote data pertinent to the current slice
        conn.execute(
            sqlalchemy.text("delete from quotes where symbol in (:x) and quote_date >= :y;"), [{'x':10,'y':11},{'x':20,'y':21}]
        )
    # Insert current slice data
        par_slice[
            ['load_id','quote_date','symbol_id','symbol','open','high','low','close','volume']
            ].to_sql(
                    'quotes',
                    conn,
                    if_exists='append',
                    index=False
            )
    # Commit the transaction
        conn.commit()
    

# Since data are processed in slices, we may need to append all slices together, before inserting.
# This is to make it into a single transaction, either succeeding or failing all at once:
# Delete records where quote_date >= start date
# Insert complete new load into quotes table.
# Commit the transaction.
# Alternative approach: Delete records pertinent to the current slice symbols only. Insert current slice. Commit.



gl_start_date, gl_as_at_date = determine_dates(None)
gl_load_id = create_load_record(gl_as_at_date)

# symbols_to_process = None
"""
# Original application
df_long['symbol'] = df_long['Ticker'].map(lambda x: map_yfsymbol_to_symbol[x])
df_long['symbol_id'] = df_long['Ticker'].map(lambda x: map_yfsymbol_to_id[x])
"""
symbols_to_process, map_yfsymbol_to_id, map_yfsymbol_to_symbol = get_processed_symbols()

for nthslice in range(0,48):
    df_shaped = take_nth_slice(nthslice)
    save_nth_slice(nthslice, df_shaped)


# We may need to update load table.
with engine.connect() as conn:
    conn.execute(
        sqlalchemy.text("update load_events set load_type='P' where id=:x;"), {'x':gl_load_id}
    )
# Commit the transaction
    conn.commit()



"""
    result = conn.execute(
        sqlalchemy.text("insert into symbols(sector_id, symbol, name) values(:sector_id, :symbol, :name)"),
        [{'sector_id':l['sector_id'], 'symbol': l['symbol'], 'name': l['name']} for l in lSymbols]
        )
    conn.commit()
"""

