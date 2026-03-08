# LSE Symbols selection

# from LSE_1Off_DBSetup import engine, events_table, sectors_table, symbols_table, quotes_table
from LSE_1Off_DBSetup import engine, events_table

import datetime, sqlalchemy
import yfinance as yf
import pandas as pd

param_load_id = None

asatdate = None
startdate = None
with engine.connect() as conn:
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
    # startdate = asatdate - datetime.timedelta(days=asatdate.weekday()) -datetime.timedelta(weeks=104)
    startdate = asatdate - datetime.timedelta(days=asatdate.weekday()) -datetime.timedelta(weeks=4)
    sql_insert_expr = sqlalchemy.insert(events_table).values(load_type='P', as_at_date=asatdate)
    print( f"* D * sql_insert_expr {sql_insert_expr}" )
    result = conn.execute(sql_insert_expr)
    print( f"* D * inserted_primary_key {result.inserted_primary_key[0]}" )
    param_load_id = result.inserted_primary_key[0]
    conn.commit()

print( f"param_load_id is {param_load_id}" )

as_at_str = asatdate.strftime("%Y-%m-%d")
start_str = startdate.strftime("%Y-%m-%d")

print( f"as_at_str is {as_at_str}" )
print( f"start_str is {start_str}" )


symbols_to_process = None
with engine.connect() as conn:
    # result=conn.execute(sqlalchemy.text("select symbol from symbols limit 10;"))
    # result=conn.execute(sqlalchemy.text("select id, symbol from symbols order by symbol desc limit :lim;"), {"lim": 3})
    result=conn.execute(sqlalchemy.text("select id, symbol from symbols;"))
    # print(result.all())
    symbols_to_process = [{"id":row[0], "symbol":row[1], "yf_symbol":'.'.join([row[1], 'L'])} for row in result]
    # for row in result:
    #     print(row[0], row[1])

print( f"* D * sybols_to_process * len is {len(symbols_to_process)}, at index 0 is {symbols_to_process[0]}" )
# cur_symbols_to_process = symbols_to_process[0:31]
cur_symbols_to_process = symbols_to_process[0:3]
print( f"* D * cur_symbols_to_process * len is {len(cur_symbols_to_process)}, at index 1 is {cur_symbols_to_process[1]}" )

# df = yf.download([smb['symbol'] for smb in cur_symbols_to_process], start='2026-02-16', end='2026-02-20', interval='1d', actions=True)
df = yf.download([smb['yf_symbol'] for smb in cur_symbols_to_process], start=start_str, end=as_at_str, interval='1d', actions=True)
# print( f"* D * df.axes * {df.axes}" )
# print( f"* D * df.index * {df.index}" )

df_long = df.stack(level='Ticker', future_stack=True).rename(columns={'Volume':'volume'}).reset_index()
df_long['load_id'] = param_load_id
df_long['quote_date']=(df_long['Date']).dt.date


# ##### TODO #####
map_yfsymbol_to_id = {}
map_yfsymbol_to_symbol = {}
for symbol_dict in symbols_to_process:
    map_yfsymbol_to_id[symbol_dict['yf_symbol']] = symbol_dict['id']
    map_yfsymbol_to_symbol[symbol_dict['yf_symbol']] = symbol_dict['symbol']

df_long['symbol'] = df_long['Ticker'].map(lambda x: map_yfsymbol_to_symbol[x])
df_long['symbol_id'] = df_long['Ticker'].map(lambda x: map_yfsymbol_to_id[x])

# ##### TODO #####
# Step 14: Convert price columns from float to integer (multiply by 1,000,000)
# price_columns = ['open', 'high', 'low', 'close']
# for col in price_columns:
#     if col in df_to_insert.columns and df_to_insert[col].notna().any():
#         df_to_insert[col] = df_to_insert[col].apply(
#             lambda x: int(round(x * 1000000)) if pd.notna(x) else None
#         )

df_long['open'] = (df_long['Open']*1000000).round().astype(pd.Int64Dtype())
df_long['high'] = (df_long['High']*1000000).round().astype(pd.Int64Dtype())
df_long['low'] = (df_long['Low']*1000000).round().astype(pd.Int64Dtype())
df_long['close'] = (df_long['Close']*1000000).round().astype(pd.Int64Dtype())

"""
    result = conn.execute(
        sqlalchemy.text("insert into symbols(sector_id, symbol, name) values(:sector_id, :symbol, :name)"),
        [{'sector_id':l['sector_id'], 'symbol': l['symbol'], 'name': l['name']} for l in lSymbols]
        )
    conn.commit()
"""
with engine.connect() as conn:
    df_long[
        ['load_id','quote_date','symbol_id','symbol','open','high','low','close','volume']
        ].to_sql(
                'quotes',
                conn,
                if_exists='append',
                index=False
        )
    conn.commit()

