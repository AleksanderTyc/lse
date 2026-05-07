# Update Symbols table using YFinance functionality
# I expect this process to run no more often than monthly. Perhaps quarterly is more appropriate.

# from LSE_1Off_DBSetup import engine, events_table, sectors_table, symbols_table, quotes_table
from LSE_1Off_DBSetup import engine, events_table

import datetime, sqlalchemy
import yfinance as yf
import pandas as pd
import datetime as dt

from typing import Tuple, List, Dict, Any, Optional
import sys

from LSE_Library import get_processed_symbols

symbols_to_process, map_yfsymbol_to_id, map_yfsymbol_to_symbol, map_yfsymbol_to_noofattempts = get_processed_symbols()


from LSE_Library import get_processed_symbols

def process_nth_slice() -> None:
    """
    Import symbols (Tickers) descriptive data from YF.
    Reshape the dataframe to desired format.
    Save the data to symbols table.
    
    Tickers are processed and saved to DB one by one.
    A ticker record is updated rather than replaced.
    
    Args:
        None
        
    Returns:
        None
    """
    
    # Build space-separated string of symbols
    slice_symbols = " ".join(symbols_to_process)
    
    # Download data from YF, apply filtering using symbols_to_process and dates.
    yftickers = yf.Tickers( slice_symbols )
    
    # We have 4 possible treatements of data:
    # - Integer, multiply by 1000000
    t1 = lambda x: round(x *1000000)
    # - Integer, multiply by 100
    t2 = lambda x: round(x *100)
    # - Timestamp, convert to Date
    t3 = lambda x: dt.datetime.fromtimestamp(x).date()
    # - String, map directly
    t4 = lambda x: x

    # We have several fields to process, each with individual treatment logic:
    fieldsToProcess = [
        { 'fieldName': 'targetHighPrice',           'fieldTreat': t1 },
        { 'fieldName': 'targetLowPrice',            'fieldTreat': t1 },
        { 'fieldName': 'targetMeanPrice',           'fieldTreat': t1 },
        { 'fieldName': 'targetMedianPrice',         'fieldTreat': t1 },
        { 'fieldName': 'recommendationMean',        'fieldTreat': t1 },
        { 'fieldName': 'recommendationKey',         'fieldTreat': t4 },
        { 'fieldName': 'numberOfAnalystOpinions',   'fieldTreat': t1 },
        { 'fieldName': 'averageAnalystRating',      'fieldTreat': t4 },
        { 'fieldName': 'dividendRate',              'fieldTreat': t1 },
        { 'fieldName': 'dividendYield',             'fieldTreat': t1 },
        { 'fieldName': 'exDividendDate',            'fieldTreat': t3 },
        { 'fieldName': 'payoutRatio',               'fieldTreat': t1 },
        { 'fieldName': 'fiveYearAvgDividendYield',  'fieldTreat': t1 },
        { 'fieldName': 'industryKey',               'fieldTreat': t4 },
        { 'fieldName': 'industryDisp',              'fieldTreat': t4 },
        { 'fieldName': 'sectorKey',                 'fieldTreat': t4 },
        { 'fieldName': 'sectorDisp',                'fieldTreat': t4 },
        { 'fieldName': 'country',                   'fieldTreat': t4 },
        { 'fieldName': 'marketCap',                 'fieldTreat': t2 },
        { 'fieldName': 'shortName',                 'fieldTreat': t4 },
        { 'fieldName': 'longName',                  'fieldTreat': t4 }
    ]
    
    # We will update records in DB symbols table
    with engine.connect() as conn:
    # Reformat data, populate additional columns
        for sticker in yftickers.tickers.keys():
    # In each iteration we will update one ticker
            print( f"* D * Processing ticker {sticker}" )
    # Some of the fields may not exist depending on data available and the company's profile. E.g. LLOY vs ACRM.
    # For this reason we build the dictionary of fields with their values from scratch.
    # We will use this dictionary as a parameter to SQL update clause.
            yffields = {}
            yffields['symbolid'] = map_yfsymbol_to_id[sticker]
    # We record successful update - this will be particularly important in BAU_Symbols update script.
    # Here we default the update markers to unsuccessful and then we set them accordingly if the update is successful - see below.
            yffields['lastUpdateDate'] = dt.date.fromisoformat('1900-01-02')
            yffields['attemptsSinceUpdate'] = -1
    # Test existence of key, if positive, apply treatment logic and assign, otherwise default to None.
    # As a precondition to update - do not update tickers, where there have been more than 3 attempts already
    # Test marketCap - this will determine if the update is successful and the incoming data is saved to DB.
    # Otherwise the update is unsuccessful - an attempt to update is recorded, any partial data are disregarded.
            infoDict = yftickers.tickers[sticker].info
            infoDictKeys = infoDict.keys()
            noOfAttemptsAlready = map_yfsymbol_to_noofattempts[sticker]
            dataUpdate = (noOfAttemptsAlready <= 3) and ('marketCap' in infoDictKeys)
            if dataUpdate:
                for fieldProcessed in fieldsToProcess:
                    if fieldProcessed['fieldName'] in infoDictKeys:
                        yffields[fieldProcessed['fieldName']] = (fieldProcessed['fieldTreat'])(infoDict[fieldProcessed['fieldName']])
                    else:
                        yffields[fieldProcessed['fieldName']] = None

    # marketCap does exist, consider update successful, set lastUpdateDate to today and attemptsSinceUpdate to 0.
                yffields['lastUpdateDate'] = dt.date.today()
                yffields['attemptsSinceUpdate'] = 0

    # Update relevant DB record
                conn.execute(
                    sqlalchemy.text("""
                        update symbols
                        set
                            targetHighPrice = :targetHighPrice,
                            targetLowPrice = :targetLowPrice,
                            targetMeanPrice = :targetMeanPrice,
                            targetMedianPrice = :targetMedianPrice,
                            recommendationMean = :recommendationMean,
                            recommendationKey = :recommendationKey,
                            numberOfAnalystOpinions = :numberOfAnalystOpinions,
                            averageAnalystRating = :averageAnalystRating,
                            dividendRate = :dividendRate,
                            dividendYield = :dividendYield,
                            exDividendDate = :exDividendDate,
                            payoutRatio = :payoutRatio,
                            fiveYearAvgDividendYield = :fiveYearAvgDividendYield,
                            industryKey = :industryKey,
                            industryDisp = :industryDisp,
                            sectorKey = :sectorKey,
                            sectorDisp = :sectorDisp,
                            country = :country,
                            marketCap = :marketCap,
                            shortName = :shortName,
                            longName = :longName,
                            lastUpdateDate = :lastUpdateDate,
                            attemptsSinceUpdate = :attemptsSinceUpdate
                        where id = :symbolid;
                        """), yffields
                )
            
            else:
    # This is not a successful update - either limit exceeded or marketCap missing - record attempt but no update
                yffields['attemptsSinceUpdate'] = noOfAttemptsAlready +1

    # Update relevant DB record
                conn.execute(
                    sqlalchemy.text("""
                        update symbols
                        set
                            attemptsSinceUpdate = :attemptsSinceUpdate
                        where id = :symbolid;
                        """), yffields
                )
        
    # Commit the transaction
        conn.commit()




symbols_to_process, map_yfsymbol_to_id, map_yfsymbol_to_symbol, map_yfsymbol_to_noofattempts = get_processed_symbols()
process_nth_slice()

"""
126 rows unsuccessful on 04MAY2026

for nthslice in range(0,48):
    print( f'* D * Processing slice {nthslice}, symbols {symbols_to_process[nthslice*31]} to {symbols_to_process[(nthslice+1)*31-1]}')
    df_shaped = process_nth_slice(nthslice)
    save_nth_slice(nthslice, df_shaped)

yffields['targetHighPrice'] = round(yftickers.tickers[sticker].info['targetHighPrice'] *1000000)
yffields['targetLowPrice'] = round(yftickers.tickers[sticker].info['targetLowPrice'] *1000000)
yffields['targetMeanPrice'] = round(yftickers.tickers[sticker].info['targetMeanPrice'] *1000000)
yffields['targetMedianPrice'] = round(yftickers.tickers[sticker].info['targetMedianPrice'] *1000000)
yffields['recommendationMean'] = round(yftickers.tickers[sticker].info['recommendationMean'] *1000000)
yffields['recommendationKey'] = yftickers.tickers[sticker].info['recommendationKey']
yffields['numberOfAnalystOpinions'] = round(yftickers.tickers[sticker].info['numberOfAnalystOpinions'] *1000000)
yffields['averageAnalystRating'] = yftickers.tickers[sticker].info['averageAnalystRating']
yffields['dividendRate'] = round(yftickers.tickers[sticker].info['dividendRate'] *1000000)
yffields['dividendYield'] = round(yftickers.tickers[sticker].info['dividendYield'] *1000000)
yffields['exDividendDate'] = dt.datetime.fromtimestamp(yftickers.tickers[sticker].info['exDividendDate']).date()
yffields['payoutRatio'] = round(yftickers.tickers[sticker].info['payoutRatio'] *1000000)
yffields['fiveYearAvgDividendYield'] = round(yftickers.tickers[sticker].info['fiveYearAvgDividendYield'] *1000000)
yffields['industryKey'] = yftickers.tickers[sticker].info['industryKey']
yffields['industryDisp'] = yftickers.tickers[sticker].info['industryDisp']
yffields['sectorKey'] = yftickers.tickers[sticker].info['sectorKey']
yffields['sectorDisp'] = yftickers.tickers[sticker].info['sectorDisp']
yffields['country'] = yftickers.tickers[sticker].info['country']
yffields['marketCap'] = round(yftickers.tickers[sticker].info['marketCap'] *100)
yffields['shortName'] = yftickers.tickers[sticker].info['shortName']
yffields['longName'] = yftickers.tickers[sticker].info['longName']
yffields['lastUpdateDate'] = dt.date.today()
yffields['attemptsSinceUpdate'] = 0
    # if this one does not exist, disregard other fields, set attempt to +1, do not modify lastUpdateDate

select sectorKey, industryKey, count(1), sum(marketCap)
from symbols
where lastUpdateDate > "1900-01-01"
group by sectorKey, industryKey
;


3847786400
13843246284800
1173284454400
"""
