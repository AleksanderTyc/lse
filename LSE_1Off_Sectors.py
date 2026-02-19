# LSE Sectors list
# This is a DB loading script, intended to run as a one-off
# It loads the DB with list of Industry Sectors as provided by lse.co.uk

# from LSE_1Off_DBSetup import engine, events_table, sectors_table, symbols_table, quotes_table
from LSE_1Off_DBSetup import engine

import sqlalchemy

primiarySectorList = ['Software and Computing',
'Telecommunications Equipment',
'Telecommunications Services',
'Health Care and Related Services',
'Medical Services',
'Medicine and Biotech',
'Banking',
'Finance Services',
'Brokerage Services',
'Closed End Investments',
'Life Insurance',
'Insurance',
'Real Estate Services',
'Real Estate Trusts',
'Automotive',
'Consumer Services',
'Household Goods',
'Leisure Goods',
'Personal Goods',
'Media',
'Retailers',
'Travel',
'Beverages',
'Food Products',
'Tobacco',
'Personal Care',
'Construction',
'Aerospace',
'Electronic and Electrical Equipment',
'Industrials',
'Industrial Engineering',
'Industrial Services',
'Industrial Transportation',
'Industrial Metals',
'Precious Metals',
'Industrial Chemicals',
'Fossil Fuels',
'Electricity Generation and Distribution',
'Gas and Water',
'Waste and Disposal'
]

with engine.connect() as conn:
    result = conn.execute(
        sqlalchemy.text("insert into sectors(sector_name) values(:sector_name)"),
        [{'sector_name': sname} for sname in primiarySectorList]
        )
    conn.commit()
