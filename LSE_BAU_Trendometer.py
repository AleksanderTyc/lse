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

import pandas as pd
import sqlite3
from datetime import datetime

def calculate_sma_130(start_date, end_date, db_path, symbol_ids=None):
    """
    Load quote data for a date range and calculate 130-period simple moving average.
    
    Parameters:
    start_date: string or date in 'YYYY-MM-DD' format
    end_date: string or date in 'YYYY-MM-DD' format
    db_path: path to SQLite database
    symbol_ids: optional list of symbol_ids to filter by. If None, returns all symbols
    
    Returns:
    DataFrame with symbol, quote_date, close, and sma_130 columns
    """
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Step 1: Build the SQL query
        # We need to load data from start_date - 129 days to end_date to have enough data for the 130-day SMA
        # Convert to datetime if strings
        start_dt = pd.to_datetime(start_date).date()
        end_dt = pd.to_datetime(end_date).date()
        
        # Calculate extended start date (go back 129 days before the start_date)
        # This ensures we have enough data for the 130-period SMA calculation
        extended_start = start_dt - pd.Timedelta(days=129)
        
        print(f"Loading quote data from {extended_start} to {end_dt}")
        print(f"Target date range for output: {start_dt} to {end_dt}")
        
        # Build query
        query = """
            SELECT 
                quote_date,
                symbol,
                symbol_id,
                close
            FROM quotes 
            WHERE quote_date BETWEEN ? AND ?
        """
        
        params = [extended_start, end_dt]
        
        # Add symbol_id filter if provided
        if symbol_ids:
            placeholders = ','.join(['?'] * len(symbol_ids))
            query += f" AND symbol_id IN ({placeholders})"
            params.extend(symbol_ids)
        
        # Order by date and symbol to ensure proper rolling window
        query += " ORDER BY symbol, quote_date"
        
        # Step 2: Load data into DataFrame
        df = pd.read_sql_query(query, conn, params=params, parse_dates=['quote_date'])
        
        if df.empty:
            print("No data found for the specified date range")
            return pd.DataFrame()
        
        print(f"Loaded {len(df)} rows of quote data")
        print(f"Unique symbols: {df['symbol'].nunique()}")
        
        # Step 3: Calculate 130-period simple moving average
        # Group by symbol and calculate rolling average
        # Using min_periods=130 ensures we only get SMA when we have a full window
        df['sma_130'] = df.groupby('symbol')['close'].transform(
            lambda x: x.rolling(window=130, min_periods=130).mean()
        )
        
        # Step 4: Filter to only the requested date range
        df_filtered = df[(df['quote_date'] >= start_dt) & (df['quote_date'] <= end_dt)].copy()
        
        # Step 5: Reorder columns for clarity
        result_df = df_filtered[['quote_date', 'symbol', 'symbol_id', 'close', 'sma_130']]
        
        print(f"\nResult contains {len(result_df)} rows for {result_df['symbol'].nunique()} symbols")
        print(f"Date range in result: {result_df['quote_date'].min()} to {result_df['quote_date'].max()}")
        
        return result_df
        
    except Exception as e:
        print(f"Error calculating SMA: {e}")
        raise
    finally:
        conn.close()


def calculate_sma_130_with_validation(start_date, end_date, db_path, symbol_ids=None):
    """
    Enhanced version with validation and additional statistics.
    """
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Convert dates
        start_dt = pd.to_datetime(start_date).date()
        end_dt = pd.to_datetime(end_date).date()
        
        # Extended start date for SMA calculation
        extended_start = start_dt - pd.Timedelta(days=129)
        
        print("=" * 70)
        print("CALCULATING 130-PERIOD SIMPLE MOVING AVERAGE")
        print("=" * 70)
        print(f"Target date range: {start_dt} to {end_dt}")
        print(f"Data loading range: {extended_start} to {end_dt}")
        
        # Build query with extended range
        query = """
            SELECT 
                quote_date,
                symbol,
                symbol_id,
                close
            FROM quotes 
            WHERE quote_date BETWEEN ? AND ?
        """
        
        params = [extended_start, end_dt]
        
        if symbol_ids:
            placeholders = ','.join(['?'] * len(symbol_ids))
            query += f" AND symbol_id IN ({placeholders})"
            params.extend(symbol_ids)
        
        query += " ORDER BY symbol, quote_date"
        
        # Load data
        df = pd.read_sql_query(query, conn, params=params, parse_dates=['quote_date'])
        
        if df.empty:
            print("No data found for the specified date range")
            return pd.DataFrame()
        
        print(f"\nData loaded: {len(df)} rows")
        
        # Check data quality
        print(f"\nData quality check:")
        print(f"  Unique symbols: {df['symbol'].nunique()}")
        print(f"  Date range in loaded data: {df['quote_date'].min().date()} to {df['quote_date'].max().date()}")
        
        # Check for missing dates per symbol
        symbols_with_issues = []
        for symbol in df['symbol'].unique():
            symbol_data = df[df['symbol'] == symbol]
            expected_days = (end_dt - extended_start).days + 1
            actual_days = len(symbol_data)
            if actual_days < expected_days * 0.9:  # If more than 10% missing
                symbols_with_issues.append((symbol, actual_days, expected_days))
        
        if symbols_with_issues:
            print(f"\n⚠️  Warning: Some symbols have missing data:")
            for symbol, actual, expected in symbols_with_issues[:5]:  # Show first 5
                print(f"    {symbol}: {actual}/{expected} days ({(actual/expected)*100:.1f}%)")
        
        # Calculate SMA
        print(f"\nCalculating 130-period SMA...")
        df['sma_130'] = df.groupby('symbol')['close'].transform(
            lambda x: x.rolling(window=130, min_periods=130).mean()
        )
        
        # Filter to target date range
        df_filtered = df[(df['quote_date'] >= start_dt) & (df['quote_date'] <= end_dt)].copy()
        
        # Generate statistics
        result_df = df_filtered[['quote_date', 'symbol', 'symbol_id', 'close', 'sma_130']]
        
        print(f"\nResults:")
        print(f"  Total rows: {len(result_df)}")
        print(f"  Unique symbols: {result_df['symbol'].nunique()}")
        print(f"  Date range: {result_df['quote_date'].min().date()} to {result_df['quote_date'].max().date()}")
        
        # SMA availability per symbol
        sma_available = result_df.groupby('symbol')['sma_130'].count()
        total_days = len(result_df['quote_date'].unique())
        print(f"\nSMA availability:")
        for symbol in result_df['symbol'].unique():
            sma_count = sma_available[symbol]
            sma_pct = (sma_count / total_days) * 100
            print(f"  {symbol}: {sma_count}/{total_days} days ({sma_pct:.1f}%) with SMA")
        
        # Sample of the data
        print(f"\nSample of calculated data (first 5 rows per symbol):")
        sample_df = result_df.groupby('symbol').head(5)
        print(sample_df.to_string(index=False))
        
        return result_df
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        conn.close()


def calculate_sma_130_alternative(start_date, end_date, db_path, symbol_ids=None):
    """
    Alternative implementation using a more efficient approach for large datasets.
    Uses pivot table to handle symbols as columns, which can be faster for many symbols.
    """
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Convert dates
        start_dt = pd.to_datetime(start_date).date()
        end_dt = pd.to_datetime(end_date).date()
        extended_start = start_dt - pd.Timedelta(days=129)
        
        # Load data
        query = """
            SELECT 
                quote_date,
                symbol,
                close
            FROM quotes 
            WHERE quote_date BETWEEN ? AND ?
        """
        
        params = [extended_start, end_dt]
        
        if symbol_ids:
            placeholders = ','.join(['?'] * len(symbol_ids))
            query += f" AND symbol_id IN ({placeholders})"
            params.extend(symbol_ids)
        
        df = pd.read_sql_query(query, conn, params=params, parse_dates=['quote_date'])
        
        if df.empty:
            return pd.DataFrame()
        
        # Pivot to have symbols as columns
        pivot_df = df.pivot_table(
            index='quote_date', 
            columns='symbol', 
            values='close',
            aggfunc='first'
        )
        
        # Calculate SMA for all symbols at once using rolling
        sma_df = pivot_df.rolling(window=130, min_periods=130).mean()
        
        # Filter to target date range
        sma_df = sma_df.loc[start_dt:end_dt]
        pivot_df = pivot_df.loc[start_dt:end_dt]
        
        # Convert back to long format
        result_df = pivot_df.stack().reset_index()
        result_df.columns = ['quote_date', 'symbol', 'close']
        
        sma_stacked = sma_df.stack().reset_index()
        sma_stacked.columns = ['quote_date', 'symbol', 'sma_130']
        
        # Merge close and SMA
        result_df = result_df.merge(sma_stacked, on=['quote_date', 'symbol'], how='left')
        
        # Add symbol_id if needed
        symbol_mapping = df[['symbol', 'symbol_id']].drop_duplicates().set_index('symbol')['symbol_id']
        result_df['symbol_id'] = result_df['symbol'].map(symbol_mapping)
        
        # Reorder columns
        result_df = result_df[['quote_date', 'symbol', 'symbol_id', 'close', 'sma_130']]
        
        return result_df
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        conn.close()


# Usage examples:
if __name__ == "__main__":
    # Example 1: Calculate SMA for all symbols
    start_date = "2024-03-01"
    end_date = "2024-03-31"
    db_path = "your_database.db"
    
    # Basic usage
    result = calculate_sma_130(start_date, end_date, db_path)
    print(f"\nResult shape: {result.shape}")
    print(result.head(10))
    
    # Enhanced with validation
    result_enhanced = calculate_sma_130_with_validation(start_date, end_date, db_path)
    
    # Filter by specific symbol IDs
    # result_filtered = calculate_sma_130(start_date, end_date, db_path, symbol_ids=[1, 2, 3])
    
    # Export to CSV
    # result.to_csv('sma_130_output.csv', index=False)
    
    # Calculate for a specific symbol
    # specific_symbol = "AAPL"
    # result_aapl = result[result['symbol'] == specific_symbol]
    # print(f"\nSMA for {specific_symbol}:")
    # print(result_aapl.head())