# LSE_Library.py
# Library of common codes

from typing import Tuple, List, Dict, Any, Optional
import sqlalchemy

from LSE_1Off_DBSetup import engine, events_table

# Create symbols_to_process list, mapping symbol, symbol.L and symbol_id
# Used in LSE_BAU_Quotes and LSE_1Off_Symbols and LSE_BAU_Symbols
def get_processed_symbols() -> Tuple[List[str],Dict[str,int],Dict[str,str],Dict[str,int]]:
    """
    Get symbols to process list from DB.
    Convert symbols to .L format required by YF to avoid ambiguity.
    Build and return two dictionaries to be used to map symbol column later:
    - .L symbol to symbol id
    - .L symbol to original symbol
    
    Returns:
        List[str]: Entire list of symbol.L
        Tuple[Dict[str,int],Dict[str,str],Dict[str,int]]: (
            {symbol.L -> symbol id},
            {symbol.L -> symbol}
            {symbol.L -> number of attempts}
        )
    """
    
    symbols_to_process = []
    map_yfsymbol_to_id:Dict[str,int] = {}
    map_yfsymbol_to_symbol:Dict[str,str] = {}
    map_yfsymbol_to_noofattempts:Dict[str,int] = {}
    with engine.connect() as conn:
        result=conn.execute(sqlalchemy.text("select id, symbol, attemptsSinceUpdate from symbols;"))
        for row in result:
            yf_symbol = '.'.join([row[1], 'L'])
            symbols_to_process.append(yf_symbol)
            map_yfsymbol_to_id[yf_symbol] = row[0]
            map_yfsymbol_to_symbol[yf_symbol] = row[1]
            map_yfsymbol_to_noofattempts[yf_symbol] = row[2]
    
    return( symbols_to_process, map_yfsymbol_to_id, map_yfsymbol_to_symbol, map_yfsymbol_to_noofattempts )


